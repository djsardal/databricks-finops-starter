-- Silver: AI Gateway telemetry -> silver.fact_ai_usage.
-- Shares fact_ai_usage with genie_usage.sql; rows distinguished by ai_surface.
-- This file (non-derived sources run first) creates the shared table.
-- NOTE: system.ai_gateway.usage was verified EMPTY in this workspace; the
-- transformation is deployed and will populate when telemetry appears.
-- Rendered with: {catalog}, {silver_lower}.

CREATE TABLE IF NOT EXISTS {catalog}.silver.fact_ai_usage (
  record_id STRING, cloud STRING, account_id STRING, workspace_id STRING,
  workspace_name STRING, region STRING, environment STRING,
  ai_surface STRING, event_date DATE, event_time TIMESTAMP,
  sku_name STRING, usage_quantity DECIMAL(38,18), usage_unit STRING,
  offering_type STRING, endpoint_id STRING, endpoint_name STRING,
  model_name STRING, input_tokens BIGINT, output_tokens BIGINT, total_tokens BIGINT,
  latency_ms BIGINT, status_code INT, requester STRING,
  payload_hash STRING, record_status STRING, ingestion_run_id STRING,
  config_version STRING, source_ingested_at TIMESTAMP
);

MERGE INTO {catalog}.silver.fact_ai_usage AS t
USING (
  SELECT
    b.record_id, b.cloud, b.account_id, b.workspace_id, b.workspace_name,
    b.region, b.environment,
    'ai_gateway' AS ai_surface,
    cast(b.source_payload.event_time AS DATE) AS event_date,
    b.source_payload.event_time AS event_time,
    cast(NULL AS STRING) AS sku_name,
    cast(NULL AS DECIMAL(38,18)) AS usage_quantity,
    cast(NULL AS STRING) AS usage_unit,
    cast(NULL AS STRING) AS offering_type,
    b.source_payload.endpoint_id AS endpoint_id,
    b.source_payload.endpoint_name AS endpoint_name,
    b.source_payload.destination_model AS model_name,
    b.source_payload.input_tokens AS input_tokens,
    b.source_payload.output_tokens AS output_tokens,
    b.source_payload.total_tokens AS total_tokens,
    b.source_payload.latency_ms AS latency_ms,
    b.source_payload.status_code AS status_code,
    b.source_payload.requester AS requester,
    b.payload_hash, b.record_status, b.ingestion_run_id,
    b.config_version, b.source_ingested_at
  FROM {catalog}.bronze.ai_gateway_usage_raw b
  WHERE b.source_ingested_at > timestamp '{silver_lower}'
) AS s
ON t.record_id = s.record_id
WHEN MATCHED AND t.payload_hash <> s.payload_hash THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
