-- =============================================================================
-- Pattern 5 - Federated read (Lakehouse Federation)
-- DATABRICKS-SPECIFIC throughout.
-- Docs: https://docs.databricks.com/aws/en/query-federation/database-federation
--
-- Prerequisites:
--   * CREATE CONNECTION privilege on the metastore
--   * network path from serverless / the SQL warehouse to the source
--   * a read-only account on the source, ideally against a read replica
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Connection. Always secret(), never a literal password - the connection
--    definition is readable by anyone with the right metastore privilege.
-- -----------------------------------------------------------------------------
CREATE CONNECTION IF NOT EXISTS orderhub_refdata_pg
TYPE POSTGRESQL
OPTIONS (
  host     'orderhub-ref-replica.abc123.us-east-1.rds.amazonaws.com',
  port     '5432',
  user     secret('orderhub', 'pg_user'),
  password secret('orderhub', 'pg_password')
)
COMMENT 'Read replica of the reference-data Postgres. Read-only account.';

-- -----------------------------------------------------------------------------
-- 2. Foreign catalog. Databricks keeps schema definitions in sync with the
--    source, so source-side DDL surfaces here without a migration.
-- -----------------------------------------------------------------------------
CREATE FOREIGN CATALOG IF NOT EXISTS orderhub_refdata
USING CONNECTION orderhub_refdata_pg
OPTIONS (database 'refdata');

-- -----------------------------------------------------------------------------
-- 3. Grants. The connection is a shared identity into the source system - treat
--    it as privileged and grant narrowly.
-- -----------------------------------------------------------------------------
GRANT USE CATALOG ON CATALOG orderhub_refdata TO `orderhub_engineers`;
GRANT USE SCHEMA, SELECT ON SCHEMA orderhub_refdata.public TO `orderhub_analysts`;
-- Deliberately NOT granted to everyone: every federated query costs the source DBA.

-- -----------------------------------------------------------------------------
-- 4. Use it - join live reference data to lakehouse facts, no pipeline
-- -----------------------------------------------------------------------------
SELECT
  f.order_date,
  t.territory_name,
  t.sales_region,
  sum(f.order_amount) AS order_amount
FROM orderhub_dev.gold.order_fact_daily        f
JOIN orderhub_refdata.public.sales_territory   t
  ON f.territory_code = t.territory_code
WHERE f.order_date >= current_date() - INTERVAL 30 DAYS
GROUP BY ALL;

-- -----------------------------------------------------------------------------
-- 5. Verify pushdown. This is the step people skip.
--    Look for the filter and the projection appearing in the remote query. If
--    they do not, the whole table is crossing the network on every execution.
-- -----------------------------------------------------------------------------
EXPLAIN FORMATTED
SELECT territory_code, territory_name
FROM orderhub_refdata.public.sales_territory
WHERE sales_region = 'northeast';

-- -----------------------------------------------------------------------------
-- 6. The escape hatch. When a federated query becomes a dashboard query, stop
--    federating it. A materialized view moves the cost back onto Databricks,
--    where it is visible and controllable.
-- -----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS orderhub_dev.gold.mv_sales_territory
  SCHEDULE CRON '0 0 6 * * ?'
  COMMENT 'Cached copy of federated reference data - refreshed daily at 06:00'
AS SELECT * FROM orderhub_refdata.public.sales_territory;

-- -----------------------------------------------------------------------------
-- 7. Monitoring: find federated queries that are scanning too much
-- -----------------------------------------------------------------------------
SELECT
  statement_id,
  executed_by,
  total_duration_ms,
  read_bytes,
  left(statement_text, 120) AS statement_preview
FROM system.query.history
WHERE statement_text ILIKE '%orderhub_refdata%'
  AND start_time >= current_timestamp() - INTERVAL 7 DAYS
ORDER BY read_bytes DESC
LIMIT 25;
