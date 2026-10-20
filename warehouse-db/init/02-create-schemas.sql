-- Creates the warehouse schemas and grants least-privilege access to each role.
-- Runs once, after 01-create-roles.sh, on first start with an empty data volume.

-- Database access: only our roles can connect.
-- :"DBNAME" is a psql variable holding the current database name.
REVOKE ALL ON DATABASE :"DBNAME" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"DBNAME" TO ingestion, transformation, analytics;  -- analytics_restricted inherits it
-- Ingestion stages each file in a temporary table before inserting it into raw;
-- dbt does the same for incremental models.
GRANT TEMPORARY ON DATABASE :"DBNAME" TO ingestion, transformation;

-- Nothing should live in the default public schema.
REVOKE ALL ON SCHEMA public FROM PUBLIC;

-- One schema per layer, plus audit for load logs and data-quality flags.
CREATE SCHEMA raw;
CREATE SCHEMA staging;
CREATE SCHEMA core;
CREATE SCHEMA live;
CREATE SCHEMA audit;

-- raw: written by ingestion, read by transformation
GRANT USAGE, CREATE ON SCHEMA raw TO ingestion;
GRANT USAGE ON SCHEMA raw TO transformation;

-- staging: transformation only
GRANT USAGE, CREATE ON SCHEMA staging TO transformation;

-- core: built and tested by transformation; internal, like staging
GRANT USAGE, CREATE ON SCHEMA core TO transformation;

-- live: the published copy of core, written by transformation once core has passed
-- its tests. The only business data analytics reads.
GRANT USAGE, CREATE ON SCHEMA live TO transformation;
GRANT USAGE ON SCHEMA live TO analytics;

-- audit: written by both ingestion and transformation. Analytics may only read the
-- tables granted to it explicitly (the data-quality history, see 03-create-audit-tables.sql).
GRANT USAGE, CREATE ON SCHEMA audit TO ingestion, transformation;
GRANT USAGE ON SCHEMA audit TO analytics;

-- Default privileges: grants for tables created in the future.
-- FOR ROLE names the user that will create the tables.
ALTER DEFAULT PRIVILEGES FOR ROLE ingestion_service IN SCHEMA raw
  GRANT SELECT ON TABLES TO transformation;

-- Live tables are readable by analytics_restricted only; each publish then grants
-- analytics the columns that are not restricted (dbt/macros/publish_models.sql). Fails
-- closed: a table whose grants were not applied is hidden from analytics.
ALTER DEFAULT PRIVILEGES FOR ROLE transformation_service IN SCHEMA live
  GRANT SELECT ON TABLES TO analytics_restricted;

ALTER DEFAULT PRIVILEGES FOR ROLE ingestion_service IN SCHEMA audit
  GRANT SELECT ON TABLES TO transformation;

ALTER DEFAULT PRIVILEGES FOR ROLE transformation_service IN SCHEMA audit
  GRANT SELECT ON TABLES TO ingestion;
