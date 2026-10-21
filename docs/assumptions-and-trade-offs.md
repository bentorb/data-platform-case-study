# Assumptions and trade-offs

## Assumptions

- **Tax is per unit**, like price: the source does not say. Amounts are `quantity × price`, `quantity × tax` and their sum.
- **A transaction with any missing or invalid field is not loaded into core.** It stays in staging with its error codes and is counted in the data-quality history.
- **Known alternative formats are repaired**, not rejected: identifiers with a letter prefix (`T1001`, `P101`) and dates as `DD-MM-YYYY`. Each repair is recorded as a warning.
- **Products have no master source**, so their names and history come from the transactions.
- **A transaction does not change after delivery**: the source has no status or update timestamp. A file delivered again replaces the rows of its earlier delivery.

## Trade-offs

Each is a design decision, what it gives, and what it costs.

- **Days are processed strictly in order** (`depends_on_past`), so incremental models never miss a load or apply one out of order. The cost: a failed or missing day blocks the following days until it is fixed, or marked as success to skip it.
- **One build of a model at a time:** each model's lock means two builds never collide or process the same loads twice. The cost: a hung build holds up the next build of that model until it finishes or its task times out (30 minutes).
- **Write-audit-publish:** consumers only ever see data that has passed its tests. The costs:
  - each core table is stored twice, in `core` and `live`;
  - a publish reads both copies of every table it publishes to find what changed, which grows with table size even when few rows change.
- **Restricted columns are denied by the database** (column privileges), not hidden by convention, so no query or tool can read them without the grant. The cost: `select *` fails for regular analysts on a live table with a restricted column; queries, and BI tools, must list the columns they read.
- **Each model owns its retention:** a model's data is kept as long as that model declares, and raw at least as long as the core models built on it need. The cost: an aggregate built partly from staging, such as `agg_daily_sales`'s rejected transactions, keeps that data as long as raw unless it declares its own retention.
- **Core rows expire with a delete,** as part of the model's build, with no custom materialisation. The cost: on large core tables the delete is slow and leaves dead rows for vacuum to reclaim (raw drops whole partitions instead; see partitioned facts in [Future work](future-work.md)).
- **Tables are never restructured automatically:** the loader refuses a drifted raw table or a change of `partition_by`, and incremental models fail on column changes (`on_schema_change: fail`), so no table changes shape silently. The cost: schema changes are manual, through a migration or a `--full-refresh`, and the init scripts only run on an empty volume.
- **A transaction ID is unique across files,** enforced by the fact's primary key, so a transaction is never counted twice. The cost: a transaction delivered in two different files blocks the pipeline until a file is corrected (see quarantine in [Future work](future-work.md)).

## Known gaps

- **Unclassified columns are readable by every analyst:** classifying a new sensitive column relies on whoever adds it (see classification checks in [Future work](future-work.md)).
- **Staging models are written per source**, using shared macros.
- **Recording data-quality issues has no task of its own:** it runs inside the staging model's task, and appears in that task's log and in the audit tables.
- **dbt's documentation shows the models in `core`,** where dbt builds them; consumers query the tables of the same name in `live`.
