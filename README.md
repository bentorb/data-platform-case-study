# Data platform

A containerised ELT platform built on **Airflow**, **dbt** and **PostgreSQL**, deployable with Docker Compose. Source files land in S3-compatible object storage; the platform loads them into the warehouse, then transforms them through staging into a tested dimensional model, recording every load, model run and data-quality issue along the way.

It is built as a **platform, not a single pipeline**: a pipeline is declared in a YAML config, and the framework in `lib/` turns it into an Airflow DAG with ingestion, schema-drift checks, retries, alerting, dbt transformations and data-quality monitoring. `customer_transactions` is the first pipeline built on it.

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (or Docker Engine with the Compose plugin), running.
- At least 4 GB of memory allocated to Docker. The stack uses about 1 GB at idle.
- Ports `5432`, `8080` and `9001` free on the host. If one is in use, set `WAREHOUSE_DB_PORT`, `AIRFLOW_PORT` or `LANDING_ZONE_CONSOLE_PORT` in `.env`; see [Configuration](#configuration).

## Start

```bash
docker compose up -d
docker compose ps -a
```

The first start pulls the base images, builds the Airflow and landing zone images, and takes a few minutes. Run `docker compose ps -a` until every long-running service shows `(healthy)` and the one-off services (`airflow-init`, `landing-zone-init`, `landing-zone-upload`) show `Exited (0)`. Without `-a`, `docker compose ps` lists running containers only, so the one-off services, which have already finished, do not appear.

> `docker compose up --wait` is not used: it reports an error (exit code 1) when a one-off container that no other service waits for exits, even successfully. `landing-zone-upload` is such a container. The stack itself starts correctly either way.

On first start:
- the scripts in `warehouse-db/init/` create the warehouse roles, schemas, grants and audit tables
- `landing-zone-init` creates the `landing` bucket, and `landing-zone-upload` delivers `data/customer_transactions.csv` to it as yesterday's file, `customer_transactions/<yesterday>.csv`
- `airflow-init` migrates the Airflow metadata database and creates the admin user

## Run the pipeline

Open <http://localhost:8080> and log in with `admin` / `admin`. DAGs are **paused when first created**: switch `customer_transactions_pipeline` on with the toggle next to its name. A run starts for yesterday, whose file `landing-zone-upload` delivered on start. To re-run days, backfill and keep the platform tidy, see [Operations](docs/operations.md).

The DAG's page shows the pipeline config it was built from (**Docs**), and the graph shows the load task, one task per dbt model and test, and the `publish` task.

## Look at the results

```bash
docker compose exec warehouse-db psql -U admin -d warehouse
```

```sql
select * from live.fct_transactions limit 10;    -- valid transactions
select * from live.dim_products;                 -- products and their name history
select * from live.agg_daily_sales;              -- sales per day, with completeness
select * from audit.load_log;                    -- every load attempt
select * from audit.dq_load_summary;             -- data-quality summary per load
select * from audit.dq_issue_counts;             -- rows affected by each issue code
select * from audit.dbt_run_results;             -- every dbt model and test result
select * from audit.publish_log;                 -- every publish to live
select * from staging.stg_customer_transactions
 where cardinality(dq_errors) > 0;               -- rejected rows and why
```

With the sample file, 100 rows are loaded, 39 are rejected by data-quality checks (listed in `dq_errors`) and 61 reach `live.fct_transactions`.

`live` is what consumers query: the copy of `core` published once every test has passed. `core`, where dbt builds and tests the models, is internal (see [Write-audit-publish](docs/data-modelling.md#write-audit-publish)).

## Landing zone console

Open <http://localhost:9001/rustfs/console/> and log in with `rustfsadmin` / `rustfsadmin`. The `landing` bucket contains the uploaded files. (`localhost:9001/` without the path returns 403.)

To deliver the files in `data/` again, as yesterday's, for example after changing one, or as another day's with `DELIVERY_DATE`:

```bash
docker compose run --rm landing-zone-upload
docker compose run --rm -e DELIVERY_DATE=2026-10-15 landing-zone-upload
```

To load a changed file, run its day again (see [Operations](docs/operations.md)). The S3 API (port 9000) is reachable only from other containers, not from the host.

## Connect to the warehouse

From the host, with any PostgreSQL client (DBeaver, TablePlus, pgAdmin, `psql`):

| Setting | Value |
|---|---|
| Host | `localhost` |
| Port | `5432` |
| Database | `warehouse` |
| User / password | `admin` / `postgres`, or a service user such as `analytics_service` / `analytics`: see [Configuration](#configuration) for every default, and [Roles and users](docs/warehouse.md#roles-and-users) for what each can access |

## Smoke test

The `warehouse_connection_check` DAG runs `select current_user, current_database(), version()` against the warehouse as `ingestion_service`. Like every DAG, it is paused when first created: switch it on, then trigger it from the UI, to check that Airflow can reach the warehouse. A run triggered while the DAG is paused waits, queued, until it is switched on.

## Tests

```bash
docker compose run --rm lint                       # static checks: Python, SQL, YAML, shell
docker compose run --rm tests                      # all tests; those needing the warehouse need the stack running
./warehouse-db/tests/test_permissions.sh           # permissions, with the stack running
```

The tests and the linters run in the Airflow image plus the test packages, against the working copies of the code; arguments to `tests` are passed to pytest (e.g. `docker compose run --rm tests tests/unit -k drift`). The permission tests check that each warehouse user can do what it should and nothing more; their test tables are cleaned up automatically. All three exit non-zero on failure. See [Testing](docs/testing.md).

## Logs

Task logs are shown in the Airflow UI under each task attempt. They are stored in the `airflow_logs` volume. Service logs (startup, errors):

```bash
docker compose logs -f airflow-scheduler
```

## Stop, reset and rebuild

```bash
docker compose down      # stop and remove containers; data and logs are kept
docker compose down -v   # also delete all volumes: databases, landed files and logs start from scratch
```

Images are built once and reused. Rebuild them after changing `airflow/Dockerfile`, `dbt/packages.yml` or anything in `landing-zone/`:

```bash
docker compose up -d --build
```

`lib/`, `pipelines/`, `airflow/dags/` and `dbt/` are mounted into the Airflow containers, so changes to them apply without a rebuild.

`down` and `down -v` do not remove built images. To remove them too, including the test image built by `lint` and `tests`:

```bash
docker compose --profile test down --rmi local
```

## Configuration

Every setting has a default in `docker-compose.yml`, so no `.env` file is needed. To override, copy `.env.example` to `.env` and edit it.

| Variable | Default | Purpose |
|---|---|---|
| `WAREHOUSE_DB_USER` | `admin` | Warehouse superuser name |
| `WAREHOUSE_DB_PASSWORD` | `postgres` | Warehouse superuser password |
| `WAREHOUSE_DB_NAME` | `warehouse` | Warehouse database name |
| `WAREHOUSE_DB_PORT` | `5432` | Warehouse host port |
| `WAREHOUSE_INGESTION_PASSWORD` | `ingestion` | Password for `ingestion_service` (also used by Airflow's `warehouse_ingestion` connection) |
| `WAREHOUSE_TRANSFORMATION_PASSWORD` | `transformation` | Password for `transformation_service` (also used by dbt) |
| `WAREHOUSE_ANALYTICS_PASSWORD` | `analytics` | Password for `analytics_service` |
| `WAREHOUSE_MAINTENANCE_PASSWORD` | `maintenance` | Password for `maintenance_service`, which expires old audit entries |
| `WAREHOUSE_ANALYTICS_RESTRICTED_PASSWORD` | `analytics_restricted` | Password for `analytics_restricted_service`, which can also read restricted columns |
| `LANDING_ZONE_ACCESS_KEY` | `rustfsadmin` | Landing zone root access key (also the console user) |
| `LANDING_ZONE_SECRET_KEY` | `rustfsadmin` | Landing zone root secret key (also the console password) |
| `LANDING_ZONE_CONSOLE_PORT` | `9001` | Landing zone console host port |
| `AIRFLOW_DB_PASSWORD` | `airflow` | Airflow metadata database password |
| `AIRFLOW_ADMIN_USER` | `admin` | Airflow UI admin user |
| `AIRFLOW_ADMIN_PASSWORD` | `admin` | Airflow UI admin password |
| `AIRFLOW_PORT` | `8080` | Airflow UI host port |
| `AIRFLOW_FERNET_KEY` | committed dev key | Encrypts secrets stored in the Airflow metadata DB |
| `AIRFLOW_JWT_SECRET` | `local-dev-jwt-secret` | Signs the tokens tasks use to talk to the API server |
| `DATA_PLATFORM_ALERT_WEBHOOK_URL` | unset | Webhook for failures and dbt test warnings of tasks owned by `data-platform` (connection `alerts_data_platform`) |
| `ALERT_WEBHOOK_URL` | unset | Default webhook, for owners without their own; with neither, alerts are only logged |

> Database credentials, roles, audit tables, the Airflow admin user and the landing zone root credentials are created only when the volumes are first created. After changing them, run `docker compose down -v`.

## Documentation

- [Architecture](docs/architecture.md)
- [Platform framework](docs/platform-framework.md)
- [dbt and data modelling](docs/data-modelling.md)
- [Airflow](docs/airflow.md)
- [Warehouse](docs/warehouse.md)
- [Landing zone](docs/landing-zone.md)
- [Operations](docs/operations.md)
- [Assumptions and trade-offs](docs/assumptions-and-trade-offs.md)
- [Performance and optimizations](docs/performance-and-optimizations.md)
- [Testing](docs/testing.md)
- [Credentials and versions](docs/credentials-and-versions.md)
- [Future work](docs/future-work.md)
