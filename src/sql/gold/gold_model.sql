-- Gold: enterprise FinOps model over CENTRAL silver facts.
-- Rebuild strategy: deterministic CREATE OR REPLACE TABLE from central Silver
-- (small datasets; idempotent; change data flows region -> central via publisher).
-- cost basis is LIST-COST ESTIMATE ONLY and is never treated as invoice cost.
-- Rendered with: {central_catalog}, {regional_catalog}, {allocation_version}.

CREATE SCHEMA IF NOT EXISTS {central_catalog}.gold;

-- Usage fact enriched with effective unit list price.
CREATE OR REPLACE TABLE {central_catalog}.gold.fact_usage_cost AS
SELECT
  u.record_id, u.cloud, u.account_id, u.workspace_id, u.workspace_name,
  u.region, u.environment,
  u.sku_name, u.usage_date, u.usage_start_time, u.usage_end_time, u.usage_unit,
  u.usage_quantity, u.record_type, u.usage_type, u.billing_origin_product,
  u.is_serverless, u.is_photon, u.job_id, u.job_run_id, u.cluster_id, u.cluster_name,
  u.warehouse_id, u.app_name, u.notebook_id, u.dlt_pipeline_id, u.endpoint_name,
  u.run_as, u.custom_tags, u.record_status,
  coalesce(p.price_effective_list, p.price_default) AS unit_list_price,
  cast(u.usage_quantity AS DECIMAL(38,6))
    * cast(coalesce(p.price_effective_list, p.price_default) AS DECIMAL(38,6)) AS list_cost_estimate,
  u.source_ingested_at
FROM {central_catalog}.silver.fact_usage u
LEFT JOIN {central_catalog}.silver.fact_price p
  ON p.sku_name = u.sku_name
 AND p.cloud = u.cloud
 AND (p.usage_unit = u.usage_unit
      OR (p.usage_unit IS NULL AND u.usage_unit IS NULL))
 AND p.price_start_time <= u.usage_start_time
 AND (p.price_end_time IS NULL OR p.price_end_time > u.usage_end_time)
WHERE u.record_status <> 'quarantined';

-- Daily workspace/sku rollup.
CREATE OR REPLACE TABLE {central_catalog}.gold.fact_cost_daily AS
SELECT
  u.usage_date, u.workspace_id, u.workspace_name, u.region, u.environment,
  u.billing_origin_product, u.sku_name, u.is_serverless,
  count(DISTINCT u.record_id) AS usage_records,
  sum(u.usage_quantity) AS usage_quantity,
  sum(u.list_cost_estimate) AS list_cost_estimate
FROM {central_catalog}.gold.fact_usage_cost u
GROUP BY u.usage_date, u.workspace_id, u.workspace_name, u.region, u.environment,
         u.billing_origin_product, u.sku_name, u.is_serverless;

-- Cost allocation: every slice records method + version + confidence.
-- direct_tag per conf/mappings/cost_allocation.yml (tag key cost_center).
-- Unassigned spend stays UNALLOCATED (never silently redistributed).
-- workspace_mapping requires a workspace registry table (documented extension point).
CREATE OR REPLACE TABLE {central_catalog}.gold.fact_cost_allocation AS
SELECT
  c.record_id, c.workspace_id, c.workspace_name, c.usage_date, c.sku_name,
  c.billing_origin_product, c.list_cost_estimate,
  coalesce(c.custom_tags['cost_center'], 'UNALLOCATED') AS cost_center,
  CASE WHEN c.custom_tags['cost_center'] IS NOT NULL
       THEN 'direct_tag' ELSE 'unallocated' END AS allocation_method,
  CASE WHEN c.custom_tags['cost_center'] IS NOT NULL
       THEN 0.9 ELSE 0.0 END AS allocation_confidence,
  '{allocation_version}' AS allocation_version
FROM {central_catalog}.gold.fact_usage_cost c;

-- Dimensions (small rebuilds).
CREATE OR REPLACE TABLE {central_catalog}.gold.dim_workspace AS
SELECT workspace_id, workspace_name, region, environment,
       min(usage_date) AS first_seen_usage_date
FROM {central_catalog}.gold.fact_usage_cost
GROUP BY workspace_id, workspace_name, region, environment;

CREATE OR REPLACE TABLE {central_catalog}.gold.dim_sku AS
SELECT DISTINCT sku_name, billing_origin_product, is_serverless, usage_unit
FROM {central_catalog}.gold.fact_usage_cost;

-- Ingestion health (latest source run + freshness + pipeline status).
CREATE OR REPLACE TABLE {central_catalog}.gold.fact_pipeline_health AS
WITH latest_runs AS (
  SELECT source_id, run_id, status, started_at, finished_at, rows_read,
         rows_inserted, rows_updated, rows_quarantined, error_class, error_message,
         row_number() OVER (PARTITION BY source_id ORDER BY started_at DESC) AS rn
  FROM {regional_catalog}.ops.source_run
)
SELECT r.source_id, r.run_id, r.status, r.started_at, r.finished_at,
       r.rows_read, r.rows_inserted, r.rows_updated, r.rows_quarantined,
       r.error_class, r.error_message,
       f.latest_event_time, f.lag_minutes, f.status AS freshness_status,
       p.status AS pipeline_status, p.config_version
FROM latest_runs r
LEFT JOIN {regional_catalog}.ops.freshness_status f ON f.source_id = r.source_id
LEFT JOIN {regional_catalog}.ops.pipeline_run p ON p.run_id = r.run_id
WHERE r.rn = 1;
