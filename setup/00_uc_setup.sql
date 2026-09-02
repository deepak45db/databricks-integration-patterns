-- =============================================================================
-- Setup: Unity Catalog objects for the integration patterns PoC
-- DATABRICKS-SPECIFIC throughout (Unity Catalog DDL, Volumes, row filters,
-- column masks). None of this is portable Spark SQL.
-- Docs: https://docs.databricks.com/aws/en/data-governance/unity-catalog/
--
-- Run once per environment. In the bundle, ${catalog} is substituted per target.
-- =============================================================================

-- Parameterised so dev and prod use the same script.
-- DATABRICKS: widgets / :params are resolved by the job task.
DECLARE OR REPLACE VARIABLE target_catalog STRING DEFAULT 'orderhub_dev';

-- -----------------------------------------------------------------------------
-- 1. Catalog and schemas
-- -----------------------------------------------------------------------------
CREATE CATALOG IF NOT EXISTS orderhub_dev
  COMMENT 'Integration patterns PoC - dev';

USE CATALOG orderhub_dev;

CREATE SCHEMA IF NOT EXISTS landing    COMMENT 'Volumes and checkpoints only';
CREATE SCHEMA IF NOT EXISTS bronze     COMMENT 'Raw, append-only, source-faithful';
CREATE SCHEMA IF NOT EXISTS silver     COMMENT 'Conformed and deduplicated';
CREATE SCHEMA IF NOT EXISTS gold       COMMENT 'Business model, consumer-facing';
CREATE SCHEMA IF NOT EXISTS quarantine COMMENT 'Rows that failed quality checks';
CREATE SCHEMA IF NOT EXISTS ops        COMMENT 'Control tables and run metadata';

-- -----------------------------------------------------------------------------
-- 2. Volumes
--    Managed volumes keep the PoC self-contained. In a client build these would
--    be EXTERNAL volumes over an existing S3 prefix with a storage credential.
-- -----------------------------------------------------------------------------
CREATE VOLUME IF NOT EXISTS landing.files
  COMMENT 'Supplier catalog and CDC file drops (P1, P2)';

CREATE VOLUME IF NOT EXISTS landing.checkpoints
  COMMENT 'Structured Streaming and Auto Loader checkpoints';

-- External volume equivalent, for reference:
-- CREATE EXTERNAL LOCATION IF NOT EXISTS orderhub_landing
--   URL 's3://my-bucket/orderhub/landing'
--   WITH (STORAGE CREDENTIAL orderhub_sc);
-- CREATE EXTERNAL VOLUME IF NOT EXISTS landing.files
--   LOCATION 's3://my-bucket/orderhub/landing/files';

-- -----------------------------------------------------------------------------
-- 3. Control tables (P4 watermark, run audit)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ops.ingest_watermark (
  source_name   STRING  NOT NULL,
  watermark_ts  TIMESTAMP,
  updated_at    TIMESTAMP,
  updated_by    STRING
) COMMENT 'High-water mark per API source (P4)';

CREATE TABLE IF NOT EXISTS ops.run_audit (
  run_id        STRING,
  pattern       STRING,
  source_name   STRING,
  rows_read     BIGINT,
  rows_written  BIGINT,
  rows_quarantined BIGINT,
  started_at    TIMESTAMP,
  ended_at      TIMESTAMP,
  status        STRING
) COMMENT 'One row per pattern execution - feeds the observability dashboard';

-- -----------------------------------------------------------------------------
-- 4. Quarantine tables
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS quarantine.supplier_catalog_bad (
  quarantined_at   TIMESTAMP,
  source_file      STRING,
  failure_reason   STRING,
  raw_payload      STRING
) COMMENT 'P1 rows failing bronze checks';

-- -----------------------------------------------------------------------------
-- 5. Groups and grants
--    Grant to groups, never to individual users. Account-level groups only, so
--    ownership survives a workspace migration.
-- -----------------------------------------------------------------------------
GRANT USE CATALOG ON CATALOG orderhub_dev TO `orderhub_engineers`;
GRANT USE SCHEMA, SELECT ON SCHEMA gold   TO `orderhub_analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA silver TO `orderhub_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA bronze     TO `orderhub_engineers`;

-- Analysts must not see bronze or quarantine: raw PII lives there.
REVOKE ALL PRIVILEGES ON SCHEMA bronze     FROM `orderhub_analysts`;
REVOKE ALL PRIVILEGES ON SCHEMA quarantine FROM `orderhub_analysts`;

-- Pipelines and jobs run as a service principal, which owns the write path.
ALTER SCHEMA bronze OWNER TO `orderhub_engineers`;
ALTER SCHEMA silver OWNER TO `orderhub_engineers`;

-- -----------------------------------------------------------------------------
-- 6. PII handling - column mask and row filter functions (P2, P6)
--    DATABRICKS: UC-native masking. Applied to the table in the pipeline or via
--    ALTER TABLE ... SET MASK.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION gold.mask_email(email STRING)
RETURN CASE
  WHEN is_account_group_member('orderhub_pii_readers') THEN email
  ELSE regexp_replace(email, '^[^@]+', '****')
END;

CREATE OR REPLACE FUNCTION gold.region_filter(region STRING)
RETURN is_account_group_member('orderhub_all_regions')
    OR region = current_user_region();  -- replace with a lookup in a real build

-- Applied later, once the tables exist:
-- ALTER TABLE silver.customer_scd2 ALTER COLUMN email SET MASK gold.mask_email;
-- ALTER TABLE gold.order_fact_daily SET ROW FILTER gold.region_filter ON (region);
