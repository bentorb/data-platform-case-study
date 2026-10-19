# Airflow image for the data platform: the official image plus the extra packages it needs.
FROM apache/airflow:3.3.2

# Airflow's constraints file pins every dependency to versions tested with this release,
# so installing extra packages cannot upgrade or downgrade Airflow's own dependencies.
ARG AIRFLOW_CONSTRAINTS=https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.13.txt

# s3fs: S3 backend for ObjectStoragePath (the local landing zone is S3-compatible).
# astronomer-cosmos: renders the dbt project as Airflow tasks.
RUN pip install --no-cache-dir \
      "apache-airflow==3.3.2" \
      "s3fs==2026.7.0" \
      "astronomer-cosmos==1.15.1" \
      --constraint "${AIRFLOW_CONSTRAINTS}"

# dbt lives in its own virtualenv so its dependencies cannot conflict with Airflow's.
# Cosmos runs it from /opt/dbt-venv/bin/dbt.
USER root
RUN python -m venv /opt/dbt-venv \
 && /opt/dbt-venv/bin/pip install --no-cache-dir \
      "dbt-core==1.12.5" \
      "dbt-postgres==1.11.0"
USER airflow
