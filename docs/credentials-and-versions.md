# Credentials and versions

## Credentials

All defaults are **local development credentials only**. They are committed so that `docker compose up` works on a fresh clone without extra setup. `.env` is gitignored so local overrides stay local.

- Published ports are bound to `127.0.0.1` only, so nothing is exposed to the network.
- Connections to the warehouse from outside its container require a password (`scram-sha-256`). Connections from inside the container are trusted, which is the image default.

In production, credentials would come from a secrets manager (e.g. AWS Secrets Manager, HashiCorp Vault, Kubernetes secrets), never from the repository: see [Airflow: connections](airflow.md#production-a-secrets-backend).

## Version pinning

Images and packages are pinned to exact versions, not `latest`, so every environment runs the same software: `postgres:18.6`, `apache/airflow:3.3.2`, `rustfs/rustfs:1.0.1`, `python:3.13.15-slim`, `boto3==1.43.108`, `astronomer-cosmos==1.15.1`, `s3fs==2026.7.0`, `dbt-core==1.12.5`, `dbt-postgres==1.11.0` and `dbt_utils` 1.4.1, and for testing `pytest==8.4.2`, `ruff==0.12.0`, `sqlfluff==3.4.0`, `yamllint==1.37.1` and `shellcheck-py==0.10.0.1`. Versions a couple of weeks old are preferred over day-old releases. Since PostgreSQL 18 the image stores data under `/var/lib/postgresql`, which is where the volumes are mounted.

Images built locally are named `data-platform-<component>:local`. The `local` tag marks them as built from the repository, not published; a published image would carry a registry prefix and a version tag.
