# dbt and data modelling

## Layers and materialisations

| Layer | Materialisation | Why |
|---|---|---|
| `staging` | View | Typing and cleaning only, always current with raw, no storage. Read only by dbt. |
| `core` | Table, or incremental for the fact and dimension | Where the dimensional model is built and tested. Internal. Every core model has an enforced contract. |
| `live` | Tables written by the publish, not by dbt | What consumers query: the published copy of core (see [Write-audit-publish](#write-audit-publish)). |
| `audit` | Platform tables, not dbt models | Operational history written by the platform (see [Audit tables](#audit-tables)). |

Each model folder builds into the warehouse schema of the same name. dbt's default would be `<target schema>_<custom schema>` (e.g. `staging_core`); `macros/generate_schema_name.sql` uses the configured name as is, because the schemas are fixed by the platform and the dbt user cannot create new ones.

### Why staging is a view

- **dbt's convention:** a staging model only types and cleans its source, one to one, and is read only by dbt; dbt recommends views for it.
- **Always in step with raw:** a file delivered again replaces its rows in raw, and retention drops whole raw partitions. A view reflects both at once. A staging table would need its own replace-by-file step, an expiry kept in step with raw's, and a full refresh whenever a parsing rule changes: more code, and two copies of the same data that could disagree.
- **No second copy** of every raw row, for a layer only dbt reads.
- **Its cost is kept to new data.** A view parses the raw rows on every read, so the daily readers only read what is new: the incremental models and the data-quality history read new loads through raw's `_load_id` index, the tests on its rows read the last 7 days through partition pruning, and the error-rate and volume checks read the data-quality history. The exception is `agg_daily_sales`, which counts every rejected row (see [Performance and optimizations](performance-and-optimizations.md) for the measurements).

It would become an incremental table once measurements showed parsing dominating run time, for example when full reads like the aggregate's grow slow.

## Data quality in staging

Every raw row is kept in staging. Each value is typed with a safe-cast macro (`macros/parsing.sql`): the value is checked against a pattern before it is cast, so an invalid value becomes null instead of failing the query.

Issues are recorded per row as arrays of codes:

| Column | Meaning | Codes |
|---|---|---|
| `dq_warnings` | A value in a known alternative format, repaired | `transaction_id_prefixed` (`T1001`), `product_id_prefixed` (`P101`), `transaction_date_dmy` (`DD-MM-YYYY`) |
| `dq_errors` | A value that cannot be used, now null | `<column>_missing` (empty), `<column>_invalid` (could not be parsed, including dates that do not exist, such as 30 February) |

**Strict rule:** only rows without errors reach `core`. A transaction without a customer, quantity or valid price is not a usable fact, and filling in defaults would make core wrong rather than incomplete. Rejected rows stay in staging with their codes, and are counted:
- `max_error_rate` (a generic test) warns when more than 10% of the newest load's rows have errors, read from the data-quality history: rejected rows make core incomplete, not wrong, so it warns rather than fails. The newest load rather than all of history, so a bad delivery shows the day it arrives
- `volume_within_range` (a generic test) warns when the newest load's row count is more than 50% away from the median of the 14 loads before it, once there are 7: a half-empty file, or one with ten times the usual rows, is often an upstream problem even when every row is valid. It reads the counts from the data-quality history, so it also sees loads whose rows were later replaced; its settings are arguments, so each source can have its own
- `agg_daily_sales` reports, per day, the transactions rejected and the share that passed (`completeness`)
- the data-quality history in `audit` records the counts for every load

## Data-quality history

`audit.dq_load_summary` (rows, rows with errors and warnings, error rate per load and staging model) and `audit.dq_issue_counts` (rows affected per load, model and issue code) are written by `macros/record_dq_issues.sql`, a **post-hook on every staging model** (`dbt_project.yml`).

- Each pipeline records the issues of its own loads, when it builds its own staging model. Monitoring is not a separate job that knows every source.
- New staging models are covered automatically: there is no list to maintain.
- The view and its data-quality rows commit in the same transaction.
- Only loads not recorded yet are inserted, those after the latest one recorded for the model (the same watermark as the incremental models), so the history is kept even for loads whose rows were later replaced by a new delivery of the same file. The filter reaches raw's `_load_id` index through the view, so only the new loads are read.

## Fact: `fct_transactions`

One row per valid transaction, with unit price and tax, net, tax and gross amounts, the product's surrogate key and the lineage columns. Every column, with its type, constraints and description, is declared in [`dbt/models/core/_core.yml`](../dbt/models/core/_core.yml).

**Amounts:** `net_amount` is `quantity × unit_price`, `tax_amount` is `quantity × unit_tax` and `gross_amount` is their sum, all exact: a whole quantity times a two-decimal unit value needs no rounding (`numeric(14,2)`). The source does not say whether tax is per unit or per transaction; it is taken to be per unit, like the price (see [Assumptions](assumptions-and-trade-offs.md#assumptions)).

It is **incremental**: each run processes only the loads that arrived since the last run, found by `_load_id` (`macros/incremental_loads.sql`):

- **New rows:** the valid rows of loads with a `_load_id` above the highest one in the fact. The logical date of the run is not used: a transaction arriving late is still processed, whatever its date.
- **Re-delivered files replace their rows, as in raw:** a pre-hook first deletes the fact's rows of every file that a new load delivered again. This also removes transactions that a new delivery dropped, or that now fail a check, which a merge on `transaction_id` would leave behind. The transaction has no status column, so a transaction is not expected to change in a later file; the same `transaction_id` in two different files fails the primary key, loudly.
- **Product key:** looked up in `dim_products` by product and transaction date. It is a left join, so a transaction without a matching product version fails the not-null constraint instead of being dropped.
- `dbt build --full-refresh --select fct_transactions` rebuilds the same table from all of raw.

## Dimension: `dim_products` (SCD type 2)

There is no product master source, so products are derived from the transactions: a product's name on a day is the name its transactions show, and a new name starts a new version.

| Column | Meaning |
|---|---|
| `product_key` | Surrogate key of the version: a hash of `product_id` and `valid_from` (`dbt_utils.generate_surrogate_key`) |
| `valid_from` / `valid_to` | The version is valid from `valid_from` (inclusive) to `valid_to` (exclusive). A product's first version starts on `1900-01-01`, so an early transaction still has a version; the current one ends on `9999-12-31`. |
| `is_current` | Whether this is the product's current version |
| `last_seen_on` | The latest transaction date showing this version's name |

The table lists the columns that carry the versioning; every column, with its type, constraints and description, is declared in [`dbt/models/core/_core.yml`](../dbt/models/core/_core.yml).

It is **incremental, and history is never rewritten**: each run reads only the new loads and the current versions. Days after a product's `last_seen_on` are applied in date order, closing the current version when the name changes. Versions and keys never change once written, so the fact's keys stay valid.

**Late data policy:** a transaction dated before a product's `last_seen_on`, whose name differs from the version covering its date, is not applied. `tests/core/dim_products_names_match_transactions.sql` warns about it, with the rows stored in `audit`. To apply such corrections, rebuild the dimension **and everything built on it**, since its keys can change:

```bash
dbt build --full-refresh --select dim_products+
```

Rebuilding the dimension alone would leave fact rows pointing at keys that no longer exist; the relationships test on `fct_transactions.product_key` catches it.

The dimension is tested for gap-free, non-overlapping versions per product (`dbt_utils.mutually_exclusive_ranges`) and one current version per product.

## Aggregates

`agg_daily_sales`, `agg_product_sales` (with the product's current name) and `agg_customer_sales` are rebuilt from the fact on every run. Each is tested with `totals_match`, a generic test checking that its transactions, units, net and gross amounts add up to the fact's, declared next to the model. Every column, with its type, constraints and description, is declared in [`dbt/models/core/aggregates/_aggregates.yml`](../dbt/models/core/aggregates/_aggregates.yml).

## One build of a model at a time

dbt cannot build the same model twice at once: table builds collide on their temporary `__dbt_tmp` and `__dbt_backup` tables, and two builds of an incremental model could both process the same loads. Two builds of a model can happen whenever two pipelines share a core model, or someone runs dbt by hand while a pipeline runs.

Every model therefore takes a lock named after it (`macros/model_lock.sql`), a Postgres advisory lock, in project-level hooks around its build (`dbt_project.yml`):

- a second build of the same model **waits** until the first has finished, then builds on its result: an incremental model then finds only what is still new
- builds of different models never wait for each other, and a build only ever holds its own lock, so builds cannot deadlock
- the hooks run outside the model's transaction, so the lock also covers what dbt does after committing, such as dropping the backup table
- a failed build's lock is released when its connection closes, at the end of its dbt process
- the publish takes the locks of the models it publishes, in name order, until it commits, so a model is not rebuilt between the publish's delete and insert

## Contracts and tests

- **Contracts:** every core model declares its columns, types and constraints (primary keys, not null) in YAML, and dbt creates the table from that declaration. A query returning other columns or types fails before anything is replaced. Keys and not-null columns are enforced by the database, so they are not tested again.
- **Exact types:** amounts are `numeric` with a fixed precision (`numeric(12,2)` per unit, `numeric(14,2)` per transaction, `numeric(16,2)` for sums), never floating point.
- **Incremental models** use `on_schema_change: fail`: a change to their columns needs a deliberate `--full-refresh`.
- **Tests** cover what constraints cannot: relationships, value ranges (`dbt_utils.accepted_range`), reconciliation, SCD2 ranges and the late-name check.
- **Data freshness:** `fct_transactions_is_recent` (`dbt_utils.recency`) warns when the newest transaction is more than 2 days old. Each daily run loads the previous day's transactions, so older data means the source is sending stale extracts, or stopped sending new ones. It warns rather than fails: stale data is still correct. `stg_customer_transactions_is_recent` checks the same on staging, which keeps every row, rejected ones included: a stale staging means the source sends old data, while a stale fact over a fresh staging means recent rows are being rejected (see the data-quality history). With the sample, whose transactions are from July 2023, both warn on every run.
- **Staging tests check recent loads:** the tests on staging's rows (`not_null`, `unique`, freshness) only check the rows loaded in the last 7 days. Older rows were tested when they were loaded, and staging is a view, so testing every row would parse all of raw on every run; `_loaded_at` is raw's partition key, so only the last week's partitions are read (see [Performance and optimizations](performance-and-optimizations.md)).
- **Failures are stored:** each test's failing rows are saved as a table in `audit` (`store_failures`), replaced on every run.
- **Results are logged:** an `on-run-end` hook (`macros/log_run_results.sql`) records every model and test result of every dbt invocation in `audit.dbt_run_results`, with the Airflow DAG, run, task and attempt that ran it.

## Write-audit-publish

Consumers never see data that has not passed its tests:

1. **Write:** dbt builds the core models in `core`, which consumers cannot read.
2. **Audit:** every test runs against them, including the tests on several models (relationships, `totals_match`).
3. **Publish:** a `publish` task at the end of each pipeline's DAG runs only if every dbt task succeeded. It copies the pipeline's core models to `live` (`macros/publish_models.sql`), **in one transaction**, so consumers see the previous or the new version of all of them, never a mix.

A failing model or test leaves `live` at its last good version; the failing task alerts as usual.

**Live tables are never replaced, only their rows change.** On a model's first publish its live table is created from the core table (`create table ... (like ... including all)`), with the same columns, types, constraints and indexes. After that, each publish syncs it on the model's primary key: rows that changed or disappeared are deleted, then changed and new rows inserted, comparing whole rows with `EXCEPT`. Because the table itself is never dropped:
- views that consumers build on live tables keep working through every publish, which dropping and recreating tables would break (`drop ... cascade` drops dependent views)
- only changed rows are written: a publish with nothing new writes nothing
- queries running during a publish are not blocked, and see the previous version until it commits

The design follows from Postgres's strengths and limits: there is no zero-copy clone to publish a tested copy for free, as on Snowflake or BigQuery, but DDL and DML are transactional, so a whole publish can commit at once. Swapping tables by renaming them would be instant, but views follow the table they were created on, so consumers' views would keep pointing at the old version.

**Rules this relies on:**
- every core model has a primary key in its contract: the publish fails otherwise
- only the publish writes to `live`, so it always converges to `core`
- a live table whose columns differ from its core model (after a model change) fails the publish, naming both column lists. It is migrated deliberately, since recreating it would drop consumers' views.

Rows older than a model's retention are deleted as the model is built, and so never published (see [Retention](warehouse.md#retention)).

Each publish also applies the live table's grants from the model's column classifications (see [Column classification](warehouse.md#column-classification)).

Every publish is recorded in `audit.publish_log`: per model, the rows deleted and inserted, whether the table was created, and the Airflow run that published it, written in the publish's own transaction.

## Audit tables

All six are created by `warehouse-db/init/03-create-audit-tables.sql`, with fixed types, keys and grants, and declared as a dbt source for documentation.

| Table | Written by | One row per |
|---|---|---|
| `load_log` | Ingestion | Load attempt |
| `dbt_run_results` | dbt's `on-run-end` hook | Model or test result of each dbt invocation |
| `dq_load_summary` | Each staging model's post-hook | Load and staging model |
| `dq_issue_counts` | Each staging model's post-hook | Load, staging model and issue code |
| `publish_log` | The publish task | Model published to live |
| `retention_log` | Retention | Table and expiry run |

Pipelines never delete entries: old ones are expired by platform maintenance (see [Retention](warehouse.md#retention)). Entries are only updated within the step that writes them (a load moving from `running` to its outcome, a publish filling in its inserted rows). The data-quality history and the publish log hold counts only, no raw values, so analysts may read them.

## Documentation

Every model and column has a description. The lineage columns are described once, in `dbt/models/_lineage_columns.md` (doc blocks), and referenced by every model that carries them. `dbt docs generate` builds the documentation site.
