# Airflow

## Components

Airflow 3 runs as separate processes, one container each: the **API server** (UI, REST API, and the execution API that running tasks report to), the **scheduler**, and the **DAG processor** (parses DAG files; part of the scheduler before Airflow 3). All of them share one configuration block in `docker-compose.yml` (`x-airflow-common`, reused with YAML anchors), so they cannot drift apart.

## Image

`airflow/Dockerfile` extends the official `apache/airflow:3.3.2` image. Following Airflow's guidance, everything is installed at build time, not when containers start:

- **Python packages:** Cosmos (dbt as Airflow tasks) and `s3fs` (the S3 backend of `ObjectStoragePath`), installed with Airflow's constraints file so they cannot change Airflow's own dependencies.
- **dbt** (`dbt-core`, `dbt-postgres`) in a separate virtualenv, so its dependencies cannot conflict with Airflow's.
- **dbt packages** from `dbt/packages.yml`, so runs need no internet access.
- **The framework and pipeline configs** (`lib/`, `pipelines/`).

A second stage, `test`, adds the test runner and the linters on top, for the `tests` and `lint` services, so tests run with exactly the platform's versions and the platform image carries no test packages.

The `dbt`, `lib` and `pipelines` folders reach the build as additional build contexts (`docker-compose.yml`), so the build context of the image stays `airflow/`.

## LocalExecutor

Tasks run as subprocesses of the scheduler. This is enough for a single machine and avoids the Redis broker, separate workers and Flower that the CeleryExecutor needs (3–4 extra containers).

**Switching to Celery** is additive and needs no changes to DAGs or the rest of the project: add a `redis` service and an `airflow-worker` service (reusing `x-airflow-common`), and set `AIRFLOW__CORE__EXECUTOR=CeleryExecutor` plus the broker and result-backend URLs. In production the CeleryExecutor or KubernetesExecutor would be used to scale tasks across machines.

## No triggerer

The triggerer is only needed for deferrable tasks (tasks that wait on an external event without holding a worker slot), and none are used. Adding it later is one more service reusing `x-airflow-common` with `command: triggerer`.

## Authentication: FAB

The FAB (Flask AppBuilder) auth manager provides a login page, user management and role-based access control with built-in roles (`Admin`, `Op`, `User`, `Viewer`, `Public`). It also supports LDAP, OAuth and SSO, which is how Airflow is usually connected to a company's identity provider. The admin user is created by `airflow-init` from `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD`.

## Startup order

```
airflow-db (healthy) ──► airflow-init (completed successfully) ──► api-server, scheduler, dag-processor
warehouse-db (healthy)
```

- `airflow-init` runs on every `docker compose up`. Its steps are idempotent: the migration and the user creation skip what already exists.
- Airflow does **not** wait for `warehouse-db`. They are independent services: Airflow should start and stay healthy even if the warehouse is down. A task that cannot reach the warehouse fails and retries, and the failure is visible in the UI.
- `airflow-db` has no published port. Only Airflow needs it, over the Compose network.

## Connections

### Locally: environment variables

Connections are defined as environment variables in `docker-compose.yml`, not created in the UI: `landing_zone_s3`, `warehouse_ingestion`, `warehouse_maintenance` and the owners' alert webhooks, e.g. `alerts_data_platform`. dbt's connection comes from `DBT_WAREHOUSE_*` variables. They are versioned with the repository, reproducible on every machine, and never stored in the metadata DB.

Airflow looks up a connection in this order: a configured **secrets backend**, then **environment variables**, then the **metadata DB**. Pipelines refer only to a connection ID, so where credentials come from is a configuration concern.

This does not scale to a platform with many resources: each one means editing the Compose file and restarting Airflow, credentials are visible to every process of every Airflow container, a warehouse password is written twice (where the user is created, and in its connection), and nothing controls, rotates or audits access to them.

### Production: a secrets backend

In production, connections come from a **secrets manager** through Airflow's secrets backend: HashiCorp Vault, AWS Secrets Manager, GCP Secret Manager or Azure Key Vault, whichever the company uses.

- **Lookup by connection ID:** Airflow reads each connection when a task needs it, from a path built from its `conn_id`, e.g. `airflow/connections/warehouse_ingestion`. Two settings configure it, `[secrets] backend` and `[secrets] backend_kwargs` (the path prefix, the authentication), so DAGs, pipeline configs and the framework do not change.
- **Naming convention:** `<system>_<role>` (`warehouse_ingestion`, `landing_zone_s3`) and `alerts_<owner>`. Connection IDs map directly to secret paths, so access policies can follow them, e.g. each team's Airflow identity reading only its own paths, and the secrets manager audits every read.
- **Created with the resources:** the infrastructure-as-code that creates a database user, a bucket or a webhook also writes its connection into the secrets manager, so a connection exists exactly when its resource does, and credentials are never handled by people. Rotating a password updates the secret, and the next task uses it.
- **One connection for Airflow and dbt:** dbt's profile is built by Cosmos from the Airflow connection (`PostgresUserPasswordProfileMapping(conn_id="warehouse_transformation")`) instead of `profiles.yml` reading environment variables, so dbt's credentials come from the same secret.
- **Caching:** every lookup is a call to the secrets manager; Airflow's secrets cache (`[secrets] use_cache`) keeps tasks from calling it for every connection access.
- **Variables** (`var.value.*`) come from the same backend, under their own path.

The only secrets left outside it are the ones Airflow needs before it can reach the backend: its metadata database connection, the Fernet key, the JWT secret and the backend's own credentials, provided by the deployment (e.g. Kubernetes secrets, or the cloud identity of the Airflow workers).

## Secrets

- **Fernet key** (`AIRFLOW_FERNET_KEY`): encrypts connection passwords and variables stored in the metadata DB. Without one, Airflow stores them unencrypted.
- **JWT secret** (`AIRFLOW_JWT_SECRET`): tasks authenticate to the execution API with tokens signed by this secret. It must be identical in every Airflow service. If each process generated its own, tasks would fail to report their state.
- Both have committed local-development defaults so the stack starts on a fresh clone. Generate your own with `openssl rand -base64 32 | tr '+/' '-_'` (Fernet) and `openssl rand -hex 32` (JWT). In production they would come from a secrets manager.
- Airflow masks connection passwords wherever they appear in task logs. Because the default password `ingestion` is part of the username `ingestion_service`, the username appears as `***_service` in logs. Masking works as intended; non-default passwords avoid the effect.

## DAGs paused at creation

New DAGs start paused (`AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true`), so nothing runs on a schedule until someone switches it on deliberately.

## Logs

Task logs are written to a named volume (`airflow_logs`), not a folder in the repository:
- The repository stays clean.
- The volume takes its ownership from the image, so there are no file-permission problems on Linux hosts and no `chown` step is needed.
- Logs are deleted together with the databases by `docker compose down -v`.

Task logs older than 90 days are deleted daily by the `platform_maintenance` DAG, and Airflow's metadata database is cleaned by the `airflow-db-clean` job (see [Retention](warehouse.md#retention)).

In production, logs would be shipped to remote storage (S3, GCS or Elasticsearch, via Airflow's `[logging]` settings) so they survive containers being replaced.
