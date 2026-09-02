-- =============================================================================
-- Observability - one place to answer "is it working and what is it costing"
-- DATABRICKS-SPECIFIC (system tables, pipeline event log)
-- Docs: https://docs.databricks.com/aws/en/admin/system-tables/
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Job health, last 7 days. The first thing anyone asks in a status meeting.
-- -----------------------------------------------------------------------------
SELECT
  t.job_id,
  max_by(t.run_name, t.period_start_time)            AS latest_run,
  count(*)                                           AS runs,
  count(*) FILTER (WHERE t.result_state = 'SUCCEEDED') AS succeeded,
  count(*) FILTER (WHERE t.result_state = 'FAILED')    AS failed,
  round(avg(timestampdiff(SECOND, t.period_start_time, t.period_end_time)), 1)
                                                     AS avg_seconds
FROM system.lakeflow.job_run_timeline t
WHERE t.period_start_time >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY t.job_id
ORDER BY failed DESC;

-- -----------------------------------------------------------------------------
-- 2. Cost attribution by pattern.
--    Requires every job and pipeline to carry a `pattern` tag - set in
--    resources/jobs.yml. Untagged spend is unattributable spend.
-- -----------------------------------------------------------------------------
SELECT
  u.custom_tags['pattern']            AS pattern,
  u.sku_name,
  round(sum(u.usage_quantity), 2)     AS dbus,
  date_trunc('DAY', u.usage_start_time) AS usage_day
FROM system.billing.usage u
WHERE u.usage_start_time >= current_timestamp() - INTERVAL 30 DAYS
  AND u.custom_tags['project'] = 'orderhub'
GROUP BY ALL
ORDER BY usage_day DESC, dbus DESC;

-- -----------------------------------------------------------------------------
-- 3. Pipeline data quality - expectation pass rates from the event log
--    Docs: https://docs.databricks.com/aws/en/ldp/observability
-- -----------------------------------------------------------------------------
SELECT
  timestamp,
  details:flow_progress.data_quality.expectations                AS expectations
FROM event_log(TABLE(orderhub_dev.silver.silver_customer_scd2))
WHERE event_type = 'flow_progress'
  AND details:flow_progress.data_quality IS NOT NULL
ORDER BY timestamp DESC
LIMIT 50;

-- -----------------------------------------------------------------------------
-- 4. Freshness - is every gold table meeting its promised refresh time?
-- -----------------------------------------------------------------------------
SELECT
  'gold.order_fact_daily' AS table_name,
  max(_built_at)          AS last_built,
  timestampdiff(MINUTE, max(_built_at), current_timestamp()) AS minutes_stale
FROM orderhub_dev.gold.order_fact_daily;

-- -----------------------------------------------------------------------------
-- 5. Quarantine trend - a rising line means the source changed and nobody said so
-- -----------------------------------------------------------------------------
SELECT
  date_trunc('DAY', started_at) AS run_day,
  pattern,
  sum(rows_written)             AS written,
  sum(rows_quarantined)         AS quarantined,
  round(100.0 * sum(rows_quarantined) / nullif(sum(rows_read), 0), 2) AS pct_bad
FROM orderhub_dev.ops.run_audit
WHERE started_at >= current_timestamp() - INTERVAL 30 DAYS
GROUP BY ALL
ORDER BY run_day DESC;

-- -----------------------------------------------------------------------------
-- 6. Who is reading what - and who has access they never use
-- -----------------------------------------------------------------------------
SELECT
  request_params.full_name_arg AS table_name,
  user_identity.email          AS principal,
  count(*)                     AS reads
FROM system.access.audit
WHERE action_name IN ('getTable', 'generateTemporaryTableCredential')
  AND event_date >= current_date() - INTERVAL 30 DAYS
  AND request_params.full_name_arg ILIKE 'orderhub_dev%'
GROUP BY ALL
ORDER BY reads DESC;
