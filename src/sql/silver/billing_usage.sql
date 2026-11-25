-- Silver: billing usage normalization -> silver.fact_usage.
-- Governance: the full source row (incl. nested structs) stays in regional Bronze;
-- Silver exposes only the modeled, non-restricted attribute set.
-- Rendered with: {catalog}, {silver_lower} (bronze source_ingested_at watermark).

CREATE TABLE IF NOT EXISTS {catalog}.silver.fact_usage (
  record_id STRING, cloud STRING, account_id STRING, workspace_id STRING,
  workspace_name STRING, region STRING, environment STRING, source_table STRING,
  sku_name STRING, usage_date DATE, usage_start_time TIMESTAMP, usage_end_time TIMESTAMP,
  usage_unit STRING, usage_quantity DECIMAL(38,18), record_type STRING, usage_type STRING,
  billing_origin_product STRING, is_serverless BOOLEAN, is_photon BOOLEAN,
  job_id STRING, job_run_id STRING, cluster_id STRING, cluster_name STRING,
  warehouse_id STRING, app_name STRING, notebook_id STRING,
  dlt_pipeline_id STRING, endpoint_name STRING, run_as STRING,
  custom_tags MAP<STRING,STRING>,
  payload_hash STRING, record_status STRING, ingestion_run_id STRING,
  config_version STRING, source_ingested_at TIMESTAMP
);

MERGE INTO {catalog}.silver.fact_usage AS t
USING (
  SELECT
    b.record_id, b.cloud, b.account_id, b.workspace_id, b.workspace_name,
    b.region, b.environment, b.source_table,
    b.source_payload.sku_name AS sku_name,
    b.source_payload.usage_date AS usage_date,
    b.source_payload.usage_start_time AS usage_start_time,
    b.source_payload.usage_end_time AS usage_end_time,
    b.source_payload.usage_unit AS usage_unit,
    b.source_payload.usage_quantity AS usage_quantity,
    b.source_payload.record_type AS record_type,
    b.source_payload.usage_type AS usage_type,
    b.source_payload.billing_origin_product AS billing_origin_product,
    b.source_payload.product_features.is_serverless AS is_serverless,
    b.source_payload.product_features.is_photon AS is_photon,
    b.source_payload.usage_metadata.job_id AS job_id,
    b.source_payload.usage_metadata.job_run_id AS job_run_id,
    b.source_payload.usage_metadata.cluster_id AS cluster_id,
    -- usage_metadata carries no cluster_name in this workspace's schema;
    -- keep the column (downstream consumers expect it) but source it as NULL.
    cast(NULL AS STRING) AS cluster_name,
    b.source_payload.usage_metadata.warehouse_id AS warehouse_id,
    b.source_payload.usage_metadata.app_name AS app_name,
    b.source_payload.usage_metadata.notebook_id AS notebook_id,
    b.source_payload.usage_metadata.dlt_pipeline_id AS dlt_pipeline_id,
    b.source_payload.usage_metadata.endpoint_name AS endpoint_name,
    b.source_payload.identity_metadata.run_as AS run_as,
    b.source_payload.custom_tags AS custom_tags,
    b.payload_hash, b.record_status, b.ingestion_run_id,
    b.config_version, b.source_ingested_at
  FROM {catalog}.bronze.billing_usage_raw b
  WHERE b.source_ingested_at > timestamp '{silver_lower}'
) AS s
ON t.record_id = s.record_id
WHEN MATCHED AND t.payload_hash <> s.payload_hash THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
