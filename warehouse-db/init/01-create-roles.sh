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
  -v analytics_password="$WAREHOUSE_ANALYTICS_PASSWORD" \
  -v analytics_restricted_password="$WAREHOUSE_ANALYTICS_RESTRICTED_PASSWORD" \
  -v maintenance_password="$WAREHOUSE_MAINTENANCE_PASSWORD" <<'EOSQL'

-- Group roles: carry privileges, cannot log in
CREATE ROLE ingestion NOLOGIN;       -- writes raw data
CREATE ROLE transformation NOLOGIN;  -- reads raw, builds staging and core, publishes live
CREATE ROLE analytics NOLOGIN;       -- read-only on live, except restricted columns
-- Everything analytics can read, plus columns classified as restricted (personal or
-- otherwise sensitive data). Granting someone that access means adding a member.
CREATE ROLE analytics_restricted NOLOGIN IN ROLE analytics;
-- Expires old audit entries; pipelines can only add to them.
CREATE ROLE maintenance NOLOGIN;

-- Login users: one per service, inheriting privileges from their group role
CREATE USER ingestion_service PASSWORD :'ingestion_password' IN ROLE ingestion;
CREATE USER transformation_service PASSWORD :'transformation_password' IN ROLE transformation;
CREATE USER analytics_service PASSWORD :'analytics_password' IN ROLE analytics;
CREATE USER analytics_restricted_service PASSWORD :'analytics_restricted_password' IN ROLE analytics_restricted;
CREATE USER maintenance_service PASSWORD :'maintenance_password' IN ROLE maintenance;

EOSQL
