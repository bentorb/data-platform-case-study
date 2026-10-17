#!/usr/bin/env bash
# Creates the warehouse roles.
#
# Group roles (NOLOGIN) hold privileges; login users are members of a group role.
# Passwords come from environment variables, which is why this is a shell script
# and not plain SQL.
#
# Runs once, on first start with an empty data volume.
set -euo pipefail

psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  -v ingestion_password="$WAREHOUSE_INGESTION_PASSWORD" \
  -v transformation_password="$WAREHOUSE_TRANSFORMATION_PASSWORD" \
  -v analytics_password="$WAREHOUSE_ANALYTICS_PASSWORD" <<'EOSQL'

-- Group roles: carry privileges, cannot log in
CREATE ROLE ingestion NOLOGIN;       -- writes raw data
CREATE ROLE transformation NOLOGIN;  -- reads raw, builds staging and core
CREATE ROLE analytics NOLOGIN;       -- read-only on core

-- Login users: one per service, inheriting privileges from their group role
CREATE USER ingestion_service PASSWORD :'ingestion_password' IN ROLE ingestion;
CREATE USER transformation_service PASSWORD :'transformation_password' IN ROLE transformation;
CREATE USER analytics_service PASSWORD :'analytics_password' IN ROLE analytics;

EOSQL
