-- =============================================================================
-- Pattern 6 - Gold model, serving views, and outbound Delta Sharing
-- DATABRICKS-SPECIFIC throughout (UC shares, recipients, row filters, masks).
-- Docs: https://docs.databricks.com/aws/en/delta-sharing/
-- =============================================================================

USE CATALOG orderhub_dev;

-- -----------------------------------------------------------------------------
-- 1. Gold fact. Business grain, contract-stable column names.
--    Joins across three patterns: P2 (customer SCD2), P3 (shipment events),
--    P5 (federated territory reference data). That cross-pattern join is the
--    whole point of the PoC - it proves the patterns compose.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE gold.order_fact_daily
COMMENT 'Daily order facts. Grain: one row per order_date / customer / region.'
TBLPROPERTIES ('quality' = 'gold', 'contract_version' = '1')
AS
SELECT
  s.event_ts::date                         AS order_date,
  c.customer_id,
  c.tier                                   AS customer_tier,
  c.region,
  s.carrier,
  count(DISTINCT s.shipment_id)            AS shipment_count,
  sum(CASE WHEN s.status = 'delivered' THEN 1 ELSE 0 END) AS delivered_count,
  current_timestamp()                      AS _built_at
FROM silver.shipment_events_dedup s
JOIN gold.gold_customer_current c
  ON s.customer_id = c.customer_id
GROUP BY ALL;

-- -----------------------------------------------------------------------------
-- 2. Quality gate. Gold failures block publication - they do not warn.
--    Run this as a job task between build_gold and refresh_share.
-- -----------------------------------------------------------------------------
SELECT
  assert_true(count(*) > 0, 'gold.order_fact_daily is empty'),
  assert_true(
    count(*) FILTER (WHERE order_date IS NULL) = 0,
    'null order_date present'
  ),
  assert_true(
    count(*) FILTER (WHERE delivered_count > shipment_count) = 0,
    'delivered exceeds total shipments'
  )
FROM gold.order_fact_daily;

-- -----------------------------------------------------------------------------
-- 3. Consumer-facing view. Share the view, never the base table - it is the only
--    way to change the physical model later without breaking every recipient.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.v1_order_fact_daily
COMMENT 'Contract v1. Additive changes only. Breaking changes ship as v2.'
AS
SELECT
  order_date,
  customer_tier,
  region,
  carrier,
  shipment_count,
  delivered_count
FROM gold.order_fact_daily;
-- customer_id deliberately excluded: partners get aggregates, not identities.

-- Row-level policy for internal consumers
ALTER TABLE gold.order_fact_daily
  SET ROW FILTER gold.region_filter ON (region);

-- -----------------------------------------------------------------------------
-- 4. The share
-- -----------------------------------------------------------------------------
CREATE SHARE IF NOT EXISTS orderhub_partner_share
  COMMENT 'Daily order aggregates for logistics partners. Contract v1.';

ALTER SHARE orderhub_partner_share
  ADD VIEW gold.v1_order_fact_daily
  COMMENT 'Refreshed daily by 07:00 ET.';

-- -----------------------------------------------------------------------------
-- 5. Recipients
--    Databricks-to-Databricks uses the sharing identifier. Open sharing issues
--    an activation link, which must be delivered out of band - never in the same
--    email as the notification that a share exists.
-- -----------------------------------------------------------------------------
CREATE RECIPIENT IF NOT EXISTS acme_logistics
  COMMENT 'Acme Logistics - contract expires 2027-06-30';
-- Databricks-to-Databricks form:
-- CREATE RECIPIENT acme_logistics USING ID 'aws:us-east-1:<their-metastore-uuid>';

GRANT SELECT ON SHARE orderhub_partner_share TO RECIPIENT acme_logistics;

-- Ownership goes to an account-level group, not to a person. A share owned by an
-- individual becomes unmanageable the day that person changes projects.
ALTER SHARE orderhub_partner_share OWNER TO `orderhub_data_stewards`;

-- -----------------------------------------------------------------------------
-- 6. Audit recipient access
-- -----------------------------------------------------------------------------
SELECT
  event_time,
  request_params.recipient_name,
  request_params.share_name,
  request_params.table_name,
  action_name
FROM system.access.audit
WHERE service_name = 'deltaSharing'
  AND event_date >= current_date() - INTERVAL 30 DAYS
ORDER BY event_time DESC;

-- -----------------------------------------------------------------------------
-- 7. Rollback. If a bad build reaches the share, restore rather than rebuild.
-- -----------------------------------------------------------------------------
-- DESCRIBE HISTORY gold.order_fact_daily;
-- RESTORE TABLE gold.order_fact_daily TO VERSION AS OF 41;
