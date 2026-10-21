# Performance and optimizations

How the platform keeps the cost of each run proportional to the data it receives, not to all the data it has ever loaded, and what was measured on the running stack to confirm it. Figures are from the sample file on Docker Desktop: absolute times are small, so they show where time goes rather than how fast a production deployment would be.

## Principles

- **A run reads what is new.** Every step that runs daily selects the new loads, by `_load_id` (the load's entry in `audit.load_log`) or by `_loaded_at` (raw's partition key), rather than scanning history.
- **Checks look at the newest load.** Data-quality checks compare the newest load with its own history in `audit`, which holds one row per load, rather than recomputing over every row.
- **History is pruned, not scanned.** Raw tables are partitioned by load time, so filters on recent loads skip older partitions, and retention drops whole partitions.
- **Nothing expensive happens while DAGs are parsed.** Parsing runs every few seconds for every DAG file, so it reads local files only.

## Airflow

### DAG parsing

| DAG file | Parse time | Tasks |
|---|---|---|
| `customer_transactions_pipeline.py` | 77 ms | 19 |
| `platform_maintenance.py` | 59 ms | 2 |
| `warehouse_connection_check.py` | 2 ms | 1 |

- **No external calls while parsing:** pipeline configs are local YAML files; connections, variables and the warehouse are only used inside tasks and callbacks.
- **dbt is not run while parsing:** Cosmos discovers the models with `dbt ls` and caches the result in Airflow's metadata database, reused while the project is unchanged (0.015 s to load).

Measured with `airflow dags report` in the DAG processor container, which parses each file and reports its duration.

### Imports

Airflow recommends keeping heavy imports out of DAG files' top level. Once Airflow and Cosmos are loaded (the DAG processor always has Airflow, and building the dbt task group needs Cosmos), the framework adds about 0.2 s, nearly all of it Cosmos's converter and operators. The modules used only by tasks (`PostgresHook`, `ObjectStoragePath`, `requests`, psycopg) take under 5 ms each, so importing them inside the tasks would save nothing measurable; they stay at the top of their modules, where they are easier to read.

Measured with `python -X importtime -c "import airflow.sdk, cosmos; import lib.pipelines.builder"`.

### dbt tasks

| | |
|---|---|
| One pipeline run (19 tasks) | 24 s |
| Each dbt task | 1.2–2.1 s |
| dbt's own work per model or test | 0.02–0.07 s |

Nearly all of a dbt task is starting a dbt process: importing dbt and connecting to the warehouse. Parsing the project is not part of it: each task reuses dbt's partial parse file, which Cosmos caches in `/tmp/cosmos` (the task log reports "Partial parse is enabled"). The start-up is the fixed price of one task per model, chosen so each model is visible, retried and alerted on its own (see [Platform framework](platform-framework.md#transformation-dbt-with-cosmos)).

Measured from Airflow's `task_instance` table (task durations), `audit.dbt_run_results` (dbt's execution time per node) and the task logs.

## Ingestion

- **One pass over the file:** it is hashed as it streams into `COPY`, in 1 MB chunks, so memory use does not grow with the file, and an unchanged file is detected without reading it twice.
- **No sorting:** rows are inserted in the order they arrive; each keeps its position in the file in `_source_row`, so nothing is gained by sorting a large file.
- **Indexed replacement:** deleting a file's earlier rows uses the index on `_source_file`, created on every partition.

## dbt

Staging models are views: each query on one parses the raw rows it reads. The daily steps that read staging, other than the aggregates, which are rebuilt in full, therefore read only new loads, verified with `explain`:

| Step | Reads | How |
|---|---|---|
| Incremental models (`fct_transactions`, `dim_products`) | New loads | `_load_id` above the model's latest load, through raw's `_load_id` index (`macros/incremental_loads.sql`) |
| Data-quality history (post-hook on staging) | New loads | `_load_id` above the latest load recorded for the model, through the same index (`macros/record_dq_issues.sql`) |
| Tests on staging's rows (`not_null`, `unique`, freshness) | Rows loaded in the last 7 days | `where: _loaded_at >= now() - interval '7 days'`: older partitions are skipped. Older rows were tested when they were loaded |
| Error rate and volume checks | The newest load and up to 14 before it | `audit.dq_load_summary`, one row per load |

How they were verified:

- **Index use:** `explain` of the data-quality post-hook's query shows `Index Only Scan ... _load_id_idx` with `Index Cond: (_load_id > (InitPlan ...))`.
- **Partition pruning:** with an old partition added, `explain analyze` of a staging test shows `Subplans Removed: 1`: the old partition is never read.
- **Only new loads recorded:** after a corrected file was delivered for the same day, the post-hook recorded the new load alone, with counts matching a direct count from staging, and kept the earlier load's history.
