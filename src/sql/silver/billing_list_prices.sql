-- Silver: published list prices -> silver.fact_price.
-- Estimated-cost inputs only; NEVER treated as invoice cost.
-- Rendered with: {catalog}, {silver_lower}.

CREATE TABLE IF NOT EXISTS {catalog}.silver.fact_price (
  record_id STRING, cloud STRING, account_id STRING, workspace_id STRING,
  region STRING, environment STRING, sku_name STRING, currency_code STRING,
  usage_unit STRING, price_start_time TIMESTAMP, price_end_time TIMESTAMP,
  price_default DECIMAL(38,12), price_promotional DECIMAL(38,12),
  price_effective_list DECIMAL(38,12),
  payload_hash STRING, record_status STRING, ingestion_run_id STRING,
  config_version STRING, source_ingested_at TIMESTAMP
);

MERGE INTO {catalog}.silver.fact_price AS t
USING (
  SELECT
    b.record_id, b.cloud, b.account_id, b.workspace_id, b.region, b.environment,
    b.source_payload.sku_name AS sku_name,
    b.source_payload.currency_code AS currency_code,
    b.source_payload.usage_unit AS usage_unit,
    b.source_payload.price_start_time AS price_start_time,
    b.source_payload.price_end_time AS price_end_time,
    b.source_payload.pricing.default AS price_default,
    b.source_payload.pricing.promotional.default AS price_promotional,
    b.source_payload.pricing.effective_list.default AS price_effective_list,
    b.payload_hash, b.record_status, b.ingestion_run_id,
    b.config_version, b.source_ingested_at
  FROM {catalog}.bronze.billing_list_prices_raw b
  WHERE b.source_ingested_at > timestamp '{silver_lower}'
) AS s
ON t.record_id = s.record_id
WHEN MATCHED AND t.payload_hash <> s.payload_hash THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
