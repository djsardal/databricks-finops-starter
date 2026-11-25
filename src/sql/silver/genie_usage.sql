-- Silver: Genie usage (derived from billing bronze) -> silver.fact_ai_usage.
-- Shares fact_ai_usage with ai_gateway_usage.sql; rows distinguished by ai_surface.
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
    'genie' AS ai_surface,
    b.source_payload.usage_date AS event_date,
    b.source_payload.usage_start_time AS event_time,
    b.source_payload.sku_name AS sku_name,
    b.source_payload.usage_quantity AS usage_quantity,
    b.source_payload.usage_unit AS usage_unit,
    coalesce(b.source_payload.product_features.genie.offering_type,
             b.source_payload.product_features.ai_bi_genie.offering_type) AS offering_type,
    cast(NULL AS STRING) AS endpoint_id,
    cast(NULL AS STRING) AS endpoint_name,
    cast(NULL AS STRING) AS model_name,
    cast(NULL AS BIGINT) AS input_tokens,
    cast(NULL AS BIGINT) AS output_tokens,
    cast(NULL AS BIGINT) AS total_tokens,
    cast(NULL AS BIGINT) AS latency_ms,
    cast(NULL AS INT) AS status_code,
    cast(NULL AS STRING) AS requester,
    b.payload_hash, b.record_status, b.ingestion_run_id,
    b.config_version, b.source_ingested_at
  FROM {catalog}.bronze.genie_usage_raw b
  WHERE b.source_ingested_at > timestamp '{silver_lower}'
) AS s
ON t.record_id = s.record_id
WHEN MATCHED AND t.payload_hash <> s.payload_hash THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
