CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS stg_analytics;
CREATE SCHEMA IF NOT EXISTS dw_marts;

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'ml_worker_role') THEN
        CREATE ROLE ml_worker_role WITH LOGIN PASSWORD 'vault_secret_pass';
    END IF;
    
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'vip_support_operator_role') THEN
        CREATE ROLE vip_support_operator_role WITH LOGIN PASSWORD 'vault_secret_pass';
    END IF;
END
$$;

REVOKE ALL ON SCHEMA public FROM ml_worker_role;
GRANT USAGE, CREATE ON SCHEMA stg_analytics TO ml_worker_role;

ALTER DEFAULT PRIVILEGES IN SCHEMA stg_analytics 
GRANT ALL ON TABLES TO ml_worker_role;

REVOKE ALL ON SCHEMA public FROM vip_support_operator_role;
GRANT USAGE ON SCHEMA dw_marts TO vip_support_operator_role;

ALTER DEFAULT PRIVILEGES IN SCHEMA dw_marts 
GRANT SELECT ON TABLES TO vip_support_operator_role;
