# Architecture

## Data flow

```
 source system ──► landing zone ──► ingestion ──► raw ──► staging ──► core ──► live ──► consumers
 (simulated)        (S3 bucket)     (lib/)          │     dbt (via Cosmos)  publish
                                       │            │        │        │       │
                                       ▼            ▼        ▼        ▼       ▼
                                     audit: load log, data-quality history, dbt results, publish log
```

One run of a pipeline DAG:

1. **Load** (`load_raw_<table>`): the day's source file is checked for schema drift, streamed into its raw table and logged in `audit.load_log`; a file identical to its latest load is skipped.
2. **Expire raw** (`expire_raw_<table>`, only when raw has a retention): deletes the files older than it.
3. **Transform** (`dbt` task group): one task per dbt model built from that raw table, each followed by its tests. Building a staging model also records its data-quality issues in `audit`; building a core model with a retention also deletes its expired rows.
4. **Publish** (`publish`): once every model and test has succeeded, the pipeline's core models are copied to `live`, the layer consumers query, in one transaction.
5. **Alert**: a task that fails after its retries is logged and posted to its owner's webhook.

## Repository layout

```
pipelines/          Pipeline configs: one YAML file per pipeline
lib/                Platform framework, imported by DAGs
  pipelines/          Builds a pipeline's DAG from its config
  ingestion/          Loads files from object storage into the warehouse
  orchestration/      Task defaults: retries, timeouts, alert callbacks
  maintenance/        Retention: expires old audit entries and task logs
  transformation/     Runs dbt as Airflow tasks (Cosmos)
airflow/
  Dockerfile          Airflow image: Cosmos, dbt, dbt packages, lib/ and pipelines/
  dags/               One small DAG file per pipeline, plus the platform's own DAGs
dbt/                dbt project: staging, core and audit sources, macros, tests
tests/              Test suite (pytest) and static checks (lint.sh)
warehouse-db/
  init/               Roles, schemas, grants and audit tables (run on first start)
  tests/              Permission tests
landing-zone/       Bucket setup and the simulated upstream delivery
data/               Sample source files, uploaded to the landing zone on start
```

## Services

| Service | Image | Purpose | Host access |
|---|---|---|---|
| `warehouse-db` | `postgres:18.6` | Warehouse: raw, staging, core, live and audit data | `localhost:5432` |
| `landing-zone-storage` | `rustfs/rustfs:1.0.1` | S3-compatible landing zone where source files are delivered | console at `localhost:9001` |
| `landing-zone-init` | `data-platform-landing-jobs:local` (built) | One-off setup: creates the `landing` bucket, then exits | – |
| `landing-zone-upload` | `data-platform-landing-jobs:local` (built) | One-off simulation of an upstream delivery: uploads the files in `data/`, then exits | – |
| `airflow-db` | `postgres:18.6` | Airflow metadata database | none (internal only) |
| `airflow-init` | `data-platform-airflow:local` (built) | One-off setup: migrates the metadata DB, creates the admin user, then exits | – |
| `airflow-api-server` | `data-platform-airflow:local` (built) | Web UI, REST API and the execution API used by running tasks | `localhost:8080` |
| `airflow-scheduler` | `data-platform-airflow:local` (built) | Schedules DAG runs and executes tasks (LocalExecutor) | – |
| `airflow-dag-processor` | `data-platform-airflow:local` (built) | Parses DAG files | – |
| `tests` | `data-platform-airflow-test:local` (built) | On demand: runs the test suite (`docker compose run --rm tests`) | – |
| `lint` | `data-platform-airflow-test:local` (built) | On demand: runs the static checks (`docker compose run --rm lint`) | – |
| `airflow-db-clean` | `data-platform-airflow:local` (built) | On demand: cleans old Airflow metadata (`docker compose run --rm airflow-db-clean`) | – |
