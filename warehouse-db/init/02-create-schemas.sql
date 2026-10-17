-- Creates the warehouse schemas and grants least-privilege access to each role.
-- Runs once, after 01-create-roles.sh, on first start with an empty data volume.

-- Database access: only our roles can connect.
-- :"DBNAME" is a psql variable holding the current database name.
REVOKE ALL ON DATABASE :"DBNAME" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"DBNAME" TO ingestion, transformation, analytics;

-- Nothing should live in the default public schema.
REVOKE ALL ON SCHEMA public FROM PUBLIC;

-- One schema per layer, plus audit for load logs and data-quality flags.
CREATE SCHEMA raw;
CREATE SCHEMA staging;
CREATE SCHEMA core;
CREATE SCHEMA audit;

-- raw: written by ingestion, read by transformation
GRANT USAGE, CREATE ON SCHEMA raw TO ingestion;
GRANT USAGE ON SCHEMA raw TO transformation;

-- staging: transformation only
GRANT USAGE, CREATE ON SCHEMA staging TO transformation;

-- core: built by transformation, read by analytics
GRANT USAGE, CREATE ON SCHEMA core TO transformation;
GRANT USAGE ON SCHEMA core TO analytics;

-- audit: written by both ingestion and transformation
GRANT USAGE, CREATE ON SCHEMA audit TO ingestion, transformation;

-- Default privileges: grants for tables created in the future.
-- FOR ROLE names the user that will create the tables.
ALTER DEFAULT PRIVILEGES FOR ROLE ingestion_service IN SCHEMA raw
  GRANT SELECT ON TABLES TO transformation;

ALTER DEFAULT PRIVILEGES FOR ROLE transformation_service IN SCHEMA core
  GRANT SELECT ON TABLES TO analytics;

ALTER DEFAULT PRIVILEGES FOR ROLE ingestion_service IN SCHEMA audit
  GRANT SELECT ON TABLES TO transformation;

ALTER DEFAULT PRIVILEGES FOR ROLE transformation_service IN SCHEMA audit
  GRANT SELECT ON TABLES TO ingestion;
