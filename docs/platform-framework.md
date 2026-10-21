# Platform framework

The framework in `lib/` is ordinary Python, imported by DAGs. Each package has one responsibility, and only `lib/pipelines/` knows about pipeline configs: the ingestion and transformation code take plain arguments, so they can also be called from hand-written DAGs, scripts or tests.

## Config-driven DAGs

Each pipeline is a YAML config plus a DAG file that calls `build_pipeline(name)`.

- **Why configs:** onboarding a source means declaring it, not writing orchestration code, and every pipeline gets the same retries, alerting, checks and monitoring.
- **One DAG file per pipeline**, rather than one file generating every DAG. Dynamic DAG generation is supported by Airflow, but it has known pitfalls, avoided here:
  - configs are **static files** in the repository, versioned and reviewed: nothing is fetched from a database or an API at parse time
  - the same config always gives the same DAG
  - a broken config only breaks its own DAG, and the import error points at its own file
  - every DAG still has a file that can be found and grepped
- **Sections next to their code:** the `ingestion` section is defined in `lib/ingestion/config.py`, `transformation` in `lib/transformation/config.py`, and `lib/pipelines/config.py` adds the pipeline-level fields. Each package owns its part of the format.
- **Strict validation** (pydantic): unknown keys are rejected, so a typo cannot be silently ignored. Tables must be `schema.table`, partitioning `day`, `month` or `none`, columns lowercase identifiers without duplicates, the owner lowercase with hyphens, a raw retention a positive number of days, and a dbt selection non-empty.
- **The DAG:** the load task, an `expire_raw_<table>` task when raw has a retention, the dbt task group, then the publish. Each run covers one interval of the schedule, e.g. the previous day, from a data-interval timetable.
- The DAG's description and owner come from the config, and the config itself is shown on the DAG's page in the UI.

## Ingestion

`lib/ingestion/object_storage_to_postgres.py` loads a CSV file from object storage into a raw table.

- **The day's file:** the source URI may be a template, e.g. `{{ data_interval_start | ds }}`, rendered by the load task for its run. Rendering is strict, and a template that cannot be rendered fails the task at once, without retrying, with the error; for a run without a logical date, Airflow 3's default for manual runs, the error also says how to give it one.
- **Storage-agnostic:** files are read through Airflow's `ObjectStoragePath`, so the URI's scheme chooses the backend (`s3://`, `gs://`, `file://`). Moving to another store is a config change.
- **Streamed:** the file is copied into PostgreSQL with `COPY`, in 1 MB chunks, so memory use does not grow with file size.
- **Every column is text** in raw, so a malformed value can never break a load: values are typed and checked in staging.
- **Lineage columns** are added to every row: `_source_file`, `_source_row` (position in the file), `_loaded_at` and `_load_id` (the load's entry in `audit.load_log`).
- **Idempotent by file:** a load deletes the rows of any earlier load of the same file and inserts the new ones, in one transaction. Re-running a load never duplicates data, and readers never see the table half-loaded.
- **Unchanged files are skipped:** the loader computes the file's SHA-256 while reading it, and records it in the load log. A file identical to its latest successful load, whose rows are still in the table, is logged as `skipped` and changes nothing: no new load reaches raw, the models find nothing new, and the publish writes nothing. If those rows are gone, e.g. expired by retention, the file is loaded again. The comparison happens within the advisory lock below.
- **Concurrent loads of the same file run one after the other**, serialised by a transaction-level advisory lock on the table and file. Without it, two loads could both delete before either inserts, leaving both copies.
- **Indexes** on `_source_file` (the delete) and `_load_id` (incremental models reading new loads) are created with the table.
- **Partitioned by load time,** per day by default, or per month, or not at all, from the config's `partition_by` (see [Warehouse: raw partitioning](warehouse.md#raw-partitioning)).

**Schema drift** (`lib/ingestion/schema_drift.py`) is checked before any data is touched:

| Change in the file | Result |
|---|---|
| Columns in a different order | Loaded: columns are matched by name |
| Extra columns | Loaded without them; they are recorded in the load's `schema_drift` and logged as a warning |
| Missing, duplicate or empty column names | Fails |
| A column named like a lineage column | Fails |
| Existing table with other columns than expected | Fails, asking for the table to be migrated first |

**Failures:** a file whose data or structure cannot be loaded (malformed CSV, schema drift) raises `AirflowFailException`, so the task fails at once without retrying, as retrying cannot fix it. Other errors, such as a database restart or a missing file, are retried.

**Load log** (`lib/ingestion/load_log.py`): every attempt gets a row in `audit.load_log`, written as `running` before the load starts and updated to `success`, `skipped` or `failed`, with row counts, the file's checksum, the error message, the extra columns, and the Airflow DAG, run, task and attempt. The start and failure are written on their own autocommitted connection, so they survive the load being rolled back; the success is written in the load's transaction, so the data and its log entry commit together.

## Orchestration defaults

Every DAG built by the framework gets the same task settings (`lib/orchestration/defaults.py`):

- **Retries:** 2, with exponential backoff from 1 minute, capped at 10 minutes. In Airflow 3 `retry_exponential_backoff` is a multiplier, not an on/off switch: `True` means 1, i.e. no backoff, so `2.0` is set explicitly.
- **Timeout:** a task running for more than 30 minutes fails, instead of blocking the DAG.
- **Alerts** (`lib/orchestration/callbacks.py`): a task that fails for good, after its retries, is logged as an error and posted to **its owner's webhook**, with the DAG, task, owner, run, attempt, error and log link. Each owner's webhook is an Airflow connection named `alerts_<owner>` (e.g. `alerts_data_platform` for `owner: data-platform`), with the URL in its extra as `{"webhook_url": ...}`, so webhook secrets stay out of the repository and can move to a secrets backend. Owners without one fall back to `ALERT_WEBHOOK_URL`. The payload (`{"text": ...}`) is the format of Google Chat and Slack incoming webhooks. A failing webhook is logged and never hides the task's own error. Retries are only logged, so nobody is paged for failures that recover. dbt tests that **warn** (e.g. data freshness, volume, error rate) do not fail their task, so they are sent to the same webhook by Cosmos's `on_warning_callback`, naming each test and its result (e.g. `fct_transactions_is_recent: Got 1 result, configured to warn if != 0`). A warning keeps alerting on every run until its cause is fixed.
- **Runs in date order** (`depends_on_past`, `wait_for_downstream`): each task waits for the same task of the previous run. Concurrent builds of a model are prevented by dbt itself, with a lock per model (see [dbt and data modelling](data-modelling.md#one-build-of-a-model-at-a-time)), so runs need no `max_active_runs` limit.

## Transformation: dbt with Cosmos

`lib/transformation/dbt.py` holds all the Cosmos configuration, so DAGs only choose which models to run.

- **One Airflow task per dbt model**, each followed by its tests (`TestBehavior.AFTER_EACH`). A failed model or test is visible, retryable and alerted on its own, and blocks only the models downstream of it.
- **Tests on several models** (e.g. relationships) get their own task, run once all their models are built, rather than being attached to one of them.
- **dbt runs in its own virtualenv** in the Airflow image (`/opt/dbt-venv`), invoked as a subprocess, so its dependencies cannot conflict with Airflow's (Cosmos's `local` execution mode). In production, dbt would run in its own image instead (see [below](#production-dbt-in-its-own-image)).
- **Model discovery:** Cosmos runs `dbt ls` when the DAG is parsed, and caches the result.
- **dbt packages** (`dbt_utils`) are installed into the image at build time. Cosmos is told not to run `dbt deps`, which it otherwise does while parsing and before every task, and which needs internet access.
- **Per-pipeline selection:** by default a pipeline runs `source:<schema>.<table>+`, the models built from its own raw table, relying on one convention: a raw table's dbt source is named after its schema. `transformation.select` overrides it. A selection that matches no models fails the DAG, as it is almost certainly a mistake.
- **No unit tests in pipelines:** dbt unit tests check code, so pipelines exclude them (`DBT_EXCLUDE_RESOURCE_TYPES=unit_test`); they run with the test suite (see [Testing](testing.md#dbt-unit-tests)).
- **Test selection is `buildable`** (`DBT_INDIRECT_SELECTION`): a test runs only when every model it tests is selected or upstream of the selection. With dbt's default (`eager`), a pipeline would also run tests of models that another pipeline builds.
- **Publish task:** after the task group, a task runs the `publish_models` macro (`dbt run-operation`) with the pipeline's selected models, publishing those in `core` to `live` (see [Write-audit-publish](data-modelling.md#write-audit-publish)). It runs only when every task in the group has succeeded.
- **Connection:** `dbt/profiles.yml` reads every value from environment variables, so it contains no credentials. The target is named `local`. In production, dbt's profile would be built from the same Airflow connection as the rest of the platform (see [Airflow: connections](airflow.md#production-a-secrets-backend)).

### Production: dbt in its own image

Locally, dbt runs inside the Airflow image, as a subprocess of the task. That is simple and fast (a task starts in about a second) and right for one machine, but it ties dbt to Airflow: upgrading dbt or adding a dbt package means rebuilding and redeploying Airflow, every dbt task competes for the same worker's CPU and memory, and the workers hold the warehouse credentials of every pipeline.

In production, Cosmos's **`kubernetes` execution mode** would run each dbt task in its own Kubernetes pod, from a dbt image (Cosmos's Kubernetes operators, built on Airflow's `KubernetesPodOperator`):

- **A versioned dbt image** (`dbt/Dockerfile`): dbt-core, dbt-postgres, the dbt packages and the project, built and published to a registry on every change, tagged with the commit. dbt is upgraded by publishing a new image, without touching Airflow.
- **DAGs parsed without dbt:** today Cosmos runs `dbt ls` while parsing. Instead, the image's build also produces the project's `manifest.json`, deployed with the DAGs, and Cosmos reads it (`LoadMode.DBT_MANIFEST`), so the Airflow image no longer needs dbt at all. The manifest and the image tag come from the same build, so the tasks Airflow renders always match the project the pods run. Cosmos then applies the pipelines' selections itself rather than dbt, so the DAG integrity tests would check that each pipeline still renders the same models and tests (including the `buildable` test selection).
- **Resources per task:** each pod declares its CPU and memory, with platform defaults and an override in the pipeline's config for heavy models, so one model can no longer starve the others, and pods spread across the cluster independently of Airflow's workers.
- **Credentials per pod:** the warehouse credentials reach the pod as environment variables from a Kubernetes secret, synced from the secrets manager (e.g. by the External Secrets Operator), so `profiles.yml` stays as it is and only dbt's pods hold the transformation user's password. Each pipeline's pods could run under their own service account.
- **Airflow context:** the run-results log (`macros/log_run_results.sql`) reads the DAG, run, task and attempt from `AIRFLOW_CTX_*` variables, which a subprocess inherits but a pod does not; they are passed to the pod explicitly, as templated environment variables.
- **The publish task** runs the same way (`DbtRunOperationKubernetesOperator`).
- **Unchanged:** the build locks and the publish's transaction live in the warehouse, so they hold wherever dbt runs; a pod that dies closes its connection, releasing its lock. The pod's output is streamed into the task's log, as today.
- **One switch:** `lib/transformation/dbt.py` would choose the execution mode from a platform setting, so DAGs and pipeline configs do not change, and `local` stays available for development.

**Costs:** a pod takes seconds to start (pulling the image, scheduling), against about a second for a subprocess, and a pipeline runs a task per model and per model's tests, so runs get slower. Running each model with its tests in one task (`TestBehavior.BUILD`, one `dbt build` per model) halves the number of pods, at the cost of retrying a model and its tests together. While a pod runs, its Airflow task only waits for it; as a deferrable task (with a triggerer) it would not hold a worker slot meanwhile. It also needs a Kubernetes cluster, which the local Docker Compose stack does not provide, so it is not built here.

Cosmos's `docker` execution mode would give the same image isolation with Docker Compose, but it needs the Docker socket mounted into Airflow, which gives Airflow full control of the host's Docker: not worth it for a local stack that `local` mode already serves.

## Deployment of the framework

`lib/` and `pipelines/` are copied into the Airflow image, at `/opt/data-platform`, which is on `PYTHONPATH`. The image therefore always holds the framework and configs it was built with. Locally, `docker-compose.yml` mounts the working copies over them, so changes apply without a rebuild.

DAG discovery safe mode is turned off (`AIRFLOW__CORE__DAG_DISCOVERY_SAFE_MODE=false`). With it on, Airflow skips files that do not mention both "airflow" and "dag", which the two-line pipeline DAG files do not. Turning it off is safe because the DAGs folder holds DAG files only: shared code lives in `lib/`.

**On managed Airflow** (Cloud Composer, MWAA) or with git-sync, usually only the DAGs folder is deployed, so `lib/` and `pipelines/` would be shipped inside it. Airflow would then parse every module in `lib/` as a possible DAG; listing `lib/` in an `.airflowignore` file in the DAGs folder prevents it.

## Adding a pipeline

A new source needs a config, a two-line DAG file and its dbt models. No orchestration or ingestion code is written.

**1. Pipeline config**: `pipelines/<name>.yml`. The file name is the pipeline's name, and its DAG is `<name>_pipeline`.

```yaml
description: Loads orders from the landing zone into the warehouse
owner: sales-data
schedule: "@daily"
start_date: 2026-01-01
ingestion:
  source:
    conn_id: landing_zone_s3
    uri: "s3://landing/orders/{{ data_interval_start | ds }}.csv"   # the file of the day the run covers
  target:
    conn_id: warehouse_ingestion
    table: raw.orders
    partition_by: month                                   # optional: day (default), month or none
  columns: [order_id, customer_id, order_date, amount]    # the columns the file must have
  # retention_days: 3650                                  # optional: keep raw longer than its core models need
transformation:                                           # optional
  select: ["source:raw.orders+"]                          # default: the models built from the target table
```

`owner` is the team that owns the pipeline: its failures and dbt test warnings are posted to that team's webhook, the Airflow connection `alerts_<owner>` (hyphens become underscores), or to the default webhook if the team has none.

The config is validated when the DAG is parsed: a misspelt key, a missing field or an invalid value fails that pipeline's DAG with an import error naming the file and the field, without affecting other pipelines.

**2. DAG file**: `airflow/dags/<name>_pipeline.py`.

```python
from lib.pipelines.builder import build_pipeline

dag = build_pipeline("orders")
```

**3. dbt models**:
- add the raw table to the `raw` source (`dbt/models/staging/_sources.yml`)
- a staging model, `stg_orders.sql`, that types and cleans the raw columns with the shared macros (`parse_integer`, `parse_decimal`, `parse_date`, `dq_error`, `dq_codes`) and keeps the lineage columns. Following the staging conventions is enough for its data-quality issues to be recorded in `audit`.
- core models built on it, with contracts and tests. Every core model needs a primary key in its contract: it is published to `live` under the same name once the pipeline's tests pass. Declare a retention on a model if its data must not be kept forever (see [Retention](warehouse.md#retention)). Classify personal or sensitive columns as `restricted` (see [Column classification](warehouse.md#column-classification)); unclassified columns are readable by every analyst.

Add unit tests for the staging model's parsing and the core models' logic (`_*unit_tests.yml`, see [Testing](testing.md#dbt-unit-tests)).

**4. Check**: `docker compose run --rm lint` and `docker compose run --rm tests`. The DAG integrity tests check that the new config builds a DAG the platform expects, and the project integrity tests that its models have contracts, primary keys, valid classifications and retentions, build locks and descriptions.

The first run creates the raw table. From then on the pipeline loads, checks, transforms and monitors the source on its schedule.
