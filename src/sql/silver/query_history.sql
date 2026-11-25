-- Silver: query history -> silver.fact_query_execution.
-- Governance: statement_text and error_message are RESTRICTED — they remain in
-- regional Bronze payloads only and are NEVER projected into Silver.
-- Rendered with: {catalog}, {silver_lower}.

CREATE TABLE IF NOT EXISTS {catalog}.silver.fact_query_execution (
  record_id STRING, cloud STRING, account_id STRING, workspace_id STRING,
  workspace_name STRING, region STRING, environment STRING,
  statement_id STRING, execution_status STRING, statement_type STRING,
  start_time TIMESTAMP, end_time TIMESTAMP, update_time TIMESTAMP,
  total_duration_ms BIGINT, compilation_duration_ms BIGINT, execution_duration_ms BIGINT,
  waiting_for_compute_duration_ms BIGINT, waiting_at_capacity_duration_ms BIGINT,
  read_rows BIGINT, read_bytes BIGINT, written_bytes BIGINT, from_result_cache BOOLEAN,
  executed_by STRING, executed_as STRING,
  payload_hash STRING, record_status STRING, ingestion_run_id STRING,
  config_version STRING, source_ingested_at TIMESTAMP
);

MERGE INTO {catalog}.silver.fact_query_execution AS t
USING (
  SELECT
    b.record_id, b.cloud, b.account_id, b.workspace_id, b.workspace_name,
    b.region, b.environment,
    b.source_payload.statement_id AS statement_id,
    b.source_payload.execution_status AS execution_status,
    b.source_payload.statement_type AS statement_type,
    b.source_payload.start_time AS start_time,
    b.source_payload.end_time AS end_time,
    b.source_payload.update_time AS update_time,
    b.source_payload.total_duration_ms AS total_duration_ms,
    b.source_payload.compilation_duration_ms AS compilation_duration_ms,
    b.source_payload.execution_duration_ms AS execution_duration_ms,
    b.source_payload.waiting_for_compute_duration_ms AS waiting_for_compute_duration_ms,
    b.source_payload.waiting_at_capacity_duration_ms AS waiting_at_capacity_duration_ms,
    b.source_payload.read_rows AS read_rows,
    b.source_payload.read_bytes AS read_bytes,
    b.source_payload.written_bytes AS written_bytes,
    b.source_payload.from_result_cache AS from_result_cache,
    b.source_payload.executed_by AS executed_by,
    b.source_payload.executed_as AS executed_as,
    b.payload_hash, b.record_status, b.ingestion_run_id,
    b.config_version, b.source_ingested_at
  FROM {catalog}.bronze.query_history_raw b
  WHERE b.source_ingested_at > timestamp '{silver_lower}'
) AS s
ON t.record_id = s.record_id
WHEN MATCHED AND t.payload_hash <> s.payload_hash THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
