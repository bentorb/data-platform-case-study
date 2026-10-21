# Warehouse

## Separate database instances

The warehouse (`warehouse-db`) and Airflow's metadata database (`airflow-db`) run in separate PostgreSQL instances.

- **Workload isolation:** heavy transformations cannot slow down the Airflow scheduler, which reads and writes its metadata DB constantly.
- **Failure isolation:** if the warehouse runs out of disk or fails, Airflow keeps running and can report the failure.
- **Independent lifecycles:** each can be upgraded, backed up and restored on its own schedule. One holds operational state, the other business data.
- **Mirrors production:** the orchestrator's metadata store and the warehouse are typically separate systems.

The extra cost is small: an idle PostgreSQL instance uses about 50–70 MB of memory.

## Schema layers

| Schema | Contents | Why |
|---|---|---|
| `raw` | Source data exactly as received, all columns as text, plus lineage columns | Source of truth: downstream layers can be rebuilt without re-extracting. Loading as text means malformed values cannot break ingestion; they are flagged later. |
| `staging` | One model per source: typed, cleaned, with data-quality codes | Cleaning happens once, in one place. No business logic. Internal to the pipeline. |
| `core` | Dimensional model (facts, dimensions) and aggregates, as built by dbt | Where models are built and tested before anyone sees them. Internal. |
| `live` | The published copy of `core` | The public interface: only data that passed every test, published per pipeline in one transaction. |
| `audit` | Load log, dbt results, data-quality history, publish and retention logs, and stored test failures | Operational metadata kept apart from business data; one place to look when something goes wrong. |

Using schemas, not table-name prefixes, means access is granted per layer and boundaries are enforced by the database. The default `public` schema is locked down so nothing is created there by accident.

## Roles and users

Least privilege: each service gets only the access it needs. Privileges are granted to **group roles** that cannot log in. **Login users** inherit them through membership, so adding a user means adding a member, with no new grants.

| Group role | Login user | raw | staging | core | live | audit |
|---|---|---|---|---|---|---|
| `ingestion` | `ingestion_service` | read / write | – | – | – | read / write (load log) |
| `transformation` | `transformation_service` | read | read / write | read / write | read / write | read / write |
| `analytics` | `analytics_service` | – | – | – | read, except restricted columns | read (data-quality history and publish log only) |
| `analytics_restricted` | `analytics_restricted_service` | – | – | – | read | as `analytics` |
| `maintenance` | `maintenance_service` | – | – | – | – | expire (delete) audit entries, record expiries |

- Group roles are named by **function** and users by the **service** that uses them, so the roles do not depend on a particular tool.
- `admin` is the superuser and is used only for administration, never by pipelines.
- Only these roles can connect to the database (`CONNECT` is revoked from `PUBLIC`). `TEMPORARY` is granted back to ingestion (its staging table for `COPY`) and transformation (dbt's incremental models).
- No pipeline user can create schemas: the set of layers is fixed by the platform, not by individual jobs.

## Default privileges

PostgreSQL grants apply only to tables that already exist, and a table belongs to the user who created it. `ALTER DEFAULT PRIVILEGES` makes access to tables created in the future automatic:

- tables `ingestion_service` creates in `raw` and `audit` are readable by `transformation`
- tables `transformation_service` creates in `live` are readable by `analytics_restricted` only, until a publish grants `analytics` their columns that are not restricted (see [Column classification](#column-classification)). A live table whose grants were never applied is hidden from `analytics`, rather than fully visible.
- tables `transformation_service` creates in `audit` are readable by `ingestion`

This matters because dbt drops and recreates tables on every run. A one-off grant would be lost after the first rebuild.

## Raw partitioning

Raw tables are partitioned by load time (`_loaded_at`), so retention drops whole partitions instead of deleting rows, and each partition's indexes stay small (`lib/ingestion/partitioning.py`). Each pipeline chooses with `ingestion.target.partition_by`:

| Value | Partitions | Suits |
|---|---|---|
| `day` (default) | One per day, e.g. `raw.orders_p20261020` | Large sources, or short raw retention |
| `month` | One per month, e.g. `raw.orders_p202610` | Small and medium sources kept for years: daily partitions would add up to thousands |
| `none` | Not partitioned | Small reference tables |

- Periods are in UTC. Before each load, in a short transaction of its own under a lock on the table, the loader creates the partitions of the current and the next period, so a load committing just after midnight still has a partition for its rows, and two loads starting a new period at once cannot both create it.
- A file loaded again moves its rows to the current partition, so a partition only ever holds files whose latest load was in its period.
- The indexes are created on the table and so on every partition.
- **Fixed once the table exists:** Postgres cannot change how a table is partitioned, so a table that differs from its pipeline's config, e.g. partitioned by day while the config says month, fails the load, asking for it to be migrated (copy into a new table partitioned as configured, then swap).

## Column classification

Access within `live` follows each column's classification, declared on the dbt model next to the column's contract:

```yaml
      - name: customer_id
        data_type: integer
        config:
          meta:
            classification: restricted
```

| Classification | Meaning | Readable by |
|---|---|---|
| `internal` (default) | Business data any analyst may use | `analytics`, `analytics_restricted` |
| `restricted` | Personal or otherwise sensitive data | `analytics_restricted` only |

- `customer_id` is restricted: an identifier that points to a person is personal data under GDPR (pseudonymised data), even without a name or an email.
- **Enforced by the database**, with column privileges: each publish revokes the analytics role's earlier grants on the table and grants it the internal columns only, in the publish's own transaction. A reclassified column takes effect on the next publish.
- `analytics_restricted` is a member of `analytics`, so it inherits everything else. Giving someone access to restricted data means adding them to that role.
- A query reading a restricted column, including `select *` on a table that has one, fails with `permission denied` for `analytics`: queries list the columns they need.
- A classification other than `internal` or `restricted` fails the publish.

## Retention

**Audit entries** are expired daily by the `platform_maintenance` DAG (`lib/maintenance/retention.py`), as `maintenance_service`, in one transaction:

| Table | Kept for |
|---|---|
| `dbt_run_results`, `publish_log` | 90 days |
| `dq_load_summary`, `dq_issue_counts` | 2 years |
| `load_log` | 2 years, except the latest successful load of each file |

- Pipelines can only add audit entries; only the `maintenance` role can delete them, and it has no access to business data.
- **Load log entries are lineage:** rows in raw and core refer to their load through `_load_id`. Loads replace whole files, so current data only refers to the latest successful load of each file, and those entries are never expired. Failed and skipped attempts and superseded loads are, once their data-quality history has been.
- Every expiry is recorded in `audit.retention_log`: the table, the cutoff and the rows deleted, with the Airflow run. `retention_log` itself is kept.
- The same DAG deletes **task logs** older than 90 days. **Airflow's metadata** (runs, task instances, XComs) is cleaned by the `airflow-db-clean` job instead (`docker compose run --rm airflow-db-clean`), because Airflow 3 tasks cannot access the metadata database; in production it would be a scheduled job, such as a Kubernetes CronJob.

**Business data** is kept forever by default. A core model can declare a retention next to its contract, often a legal requirement (a minimum for financial records, a maximum for personal data):

```yaml
  - name: fct_transactions
    config:
      meta:
        retention: {column: transaction_date, days: 2555}   # 7 years
```

- **core:** a folder-level post-hook (`dbt/macros/expire_rows.sql`) deletes the model's rows older than its retention **as part of building it**, in the same transaction. Models downstream, tests and the publish only ever see the expired data, even when an old file is reprocessed.
- **live:** the publish at the end of the same run deletes the rows no longer in core.
- **raw:** is kept **at least as long as the longest-lived core model built on it**, so a full refresh can always rebuild their history: if any of them is kept forever, so is raw. A pipeline can keep raw longer with `ingestion.retention_days`, never shorter; a shorter one fails the DAG. When raw has a retention, an `expire_raw_<table>` task, as the ingestion user, expires the files whose latest load is older than it, before the models are built: a partitioned table drops its partitions whose whole period is older, which is instant (so raw may be kept up to a period longer); a table with `partition_by: none` deletes those files' rows.
- Every expiry, of every layer, is recorded in `audit.retention_log`.

## Naming conventions

Lowercase `snake_case`, unquoted. PostgreSQL folds unquoted names to lowercase, and quoted mixed-case names must then be quoted everywhere, which is error-prone in SQL, dbt and BI tools. None of the names used are reserved words (checked against `pg_get_keywords()`).

## Initialisation scripts

`warehouse-db/init/` is mounted into the container's `/docker-entrypoint-initdb.d/`, and the scripts run in alphabetical order:

1. `01-create-roles.sh` creates the group roles and login users. It is a shell script because the passwords come from environment variables, passed to `psql` as variables so they are quoted and escaped safely.
2. `02-create-schemas.sql` creates the schemas, grants and default privileges.
3. `03-create-audit-tables.sql` creates the platform's audit tables, including `retention_log`, and their grants.

**Limitation:** these scripts run only when the data volume is empty. That is fine for a reproducible local environment. On a shared or production database, roles, schemas and platform tables change over time, so they would be managed by a migration tool (Flyway, Liquibase, Alembic) or infrastructure-as-code (e.g. Terraform's PostgreSQL provider), with changes versioned and applied incrementally.
