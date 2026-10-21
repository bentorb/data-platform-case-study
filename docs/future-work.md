# Future work

**Orchestration and scale**
- **Partitioning large facts** by date, so retention drops whole partitions and queries by date read only the partitions they need (raw tables already are). dbt-postgres cannot create partitioned tables, so it needs a custom materialisation, and a primary key that includes the partition key, which no longer guarantees `transaction_id` is unique across partitions.
- **Shared core models:** models built from several pipelines move to a transformation DAG scheduled on Airflow Assets, updated by each pipeline's load, so they are built once when their inputs change.
- **CeleryExecutor or KubernetesExecutor** to scale tasks across machines, with Cosmos running dbt in its own image (`kubernetes` execution mode), as designed in [Platform framework: dbt in its own image](platform-framework.md#production-dbt-in-its-own-image).

**Data quality and observability**
- **Delivery freshness:** check each pipeline's latest delivery and alert against missed SLAs (defined in the config).
- **Cheaper publishes for large tables:** compare only the rows of recent loads (by `_load_id`) for incremental models, and publish large models rebuilt in full by swapping a platform-owned view onto a new table, instead of comparing whole tables.
- **Quarantine duplicate transactions:** today the same `transaction_id` in two different files fails the fact's primary key and blocks the pipeline until a file is corrected.

**Governance**
- **Dynamic masking** with PostgreSQL Anonymizer: masked roles would see restricted columns masked (null, partial, hashed) instead of being denied, so `select *` keeps working. It needs a custom Postgres image with the extension; the classifications stay the same, only how the publish applies them changes.
- **Classification checks in CI:** fail when a new published column has no classification review, instead of defaulting to `internal`.
- **Landing zone lifecycle:** object storage rules moving old landing files to an archive tier and expiring them, applied as infrastructure-as-code.
- **Access auditing** with `pgaudit`: record who reads restricted columns.
- **Secrets backend:** connections and variables from a secrets manager, as designed in [Airflow: connections](airflow.md#production-a-secrets-backend), with dbt's profile built from the same Airflow connection. Locally, a `LocalFilesystemBackend` with the same connection IDs could replace the environment variables.
- **Least-privilege landing zone access:** a read-only access key for ingestion, or an IAM role in production.
- **Migrations:** roles, schemas and audit tables managed by a migration tool instead of init scripts, along with the raw table changes the loader refuses to make itself: a table whose columns changed (schema drift), or a change of `partition_by`.

**Modelling**
- **`dim_date`** for calendar attributes.
- **`intermediate` layer** for complex logic between `staging` and `core`.

**Developer experience**
- **Integration tests:** pytest tests calling the platform's components directly against real infrastructure.
- **End-to-end test:** start the stack, run a pipeline's DAG for a day, and check what it published to `live` and recorded in `audit` (loads, data-quality history, publish log), as done by hand while building the platform.
- **CI:** run `docker compose run --rm lint` and, on a fresh stack with `REQUIRE_WAREHOUSE` set, `docker compose run --rm tests` and the permission tests, on every change.
- **Pipeline config checks across the platform:** no two pipelines loading the same raw table, every target table declared as a dbt source, every connection a config uses defined in the environment, and a warning for owners without an alerts connection.
- **A JSON Schema for pipeline configs,** exported from the pydantic models and referenced at the top of each config, so editors validate and autocomplete configs as they are written.
- **Scaffolding:** `make new-pipeline name=...` creating the config, DAG file, dbt source, staging model and YAML.
