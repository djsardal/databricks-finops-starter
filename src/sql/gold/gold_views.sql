-- Gold: canonical reporting views (dashboard datasets).
-- All costs are LIST-COST ESTIMATES — never invoice cost.
-- Rendered with: {central_catalog}, {regional_catalog}.

CREATE OR REPLACE VIEW {central_catalog}.gold.vw_finops_daily_cost AS
SELECT usage_date, workspace_id, workspace_name, region, environment,
       billing_origin_product, sku_name, is_serverless,
       usage_records, usage_quantity, list_cost_estimate
FROM {central_catalog}.gold.fact_cost_daily;

CREATE OR REPLACE VIEW {central_catalog}.gold.vw_cost_by_workspace AS
SELECT workspace_id, workspace_name, region, environment,
       sum(list_cost_estimate) AS list_cost_estimate,
       min(usage_date) AS first_usage_date, max(usage_date) AS last_usage_date
FROM {central_catalog}.gold.fact_usage_cost
GROUP BY workspace_id, workspace_name, region, environment;

CREATE OR REPLACE VIEW {central_catalog}.gold.vw_cost_by_product_sku AS
SELECT billing_origin_product, sku_name, is_serverless,
       sum(list_cost_estimate) AS list_cost_estimate, sum(usage_quantity) AS usage_quantity
FROM {central_catalog}.gold.fact_usage_cost
GROUP BY billing_origin_product, sku_name, is_serverless;

CREATE OR REPLACE VIEW {central_catalog}.gold.vw_classic_vs_serverless AS
SELECT usage_date, environment, is_serverless,
       sum(list_cost_estimate) AS list_cost_estimate
FROM {central_catalog}.gold.fact_usage_cost
GROUP BY usage_date, environment, is_serverless;

-- Cost per job run. NOTE: "successful run" filtering needs Lakeflow job telemetry
-- (optional source); until that silver fact exists the view reports cost per
-- observed job run (job_id, job_run_id) from usage facts.
CREATE OR REPLACE VIEW {central_catalog}.gold.vw_cost_per_successful_job_run AS
SELECT job_id, job_run_id,
       count(DISTINCT record_id) AS usage_records,
       sum(list_cost_estimate) AS job_run_list_cost
FROM {central_catalog}.gold.fact_usage_cost
WHERE job_id IS NOT NULL AND job_run_id IS NOT NULL
GROUP BY job_id, job_run_id;

-- Genie + AI Gateway usage (both surfaces in one fact).
CREATE OR REPLACE VIEW {central_catalog}.gold.vw_genie_ai_usage AS
SELECT ai_surface, event_date, workspace_id,
       sum(usage_quantity) AS usage_quantity,
       sum(input_tokens) AS input_tokens, sum(output_tokens) AS output_tokens,
       sum(total_tokens) AS total_tokens, avg(latency_ms) AS avg_latency_ms,
       count(*) AS events
FROM {central_catalog}.silver.fact_ai_usage
GROUP BY ai_surface, event_date, workspace_id;

-- Explicit estimated-list-cost view with unambiguous cost basis label.
CREATE OR REPLACE VIEW {central_catalog}.gold.vw_estimated_list_cost AS
SELECT usage_date, region, environment,
       sum(list_cost_estimate) AS estimated_list_cost_total,
       'list_price_estimate' AS cost_basis
FROM {central_catalog}.gold.fact_usage_cost
GROUP BY usage_date, region, environment;

-- Ingestion health for the dashboard (includes open alerts per source).
CREATE OR REPLACE VIEW {central_catalog}.gold.vw_ingestion_health AS
SELECT h.source_id, h.status, h.started_at, h.finished_at,
       h.rows_read, h.rows_inserted, h.rows_quarantined,
       h.error_class, h.lag_minutes, h.freshness_status,
       h.pipeline_status, h.config_version,
       coalesce(a.open_alerts, 0) AS open_alerts
FROM {central_catalog}.gold.fact_pipeline_health h
LEFT JOIN (
  SELECT source_id, count(*) AS open_alerts
  FROM {regional_catalog}.ops.alert_outbox
  WHERE delivery_status = 'UNSENT'
  GROUP BY source_id
) a ON a.source_id = h.source_id;
