-- Bootstrap the REGIONAL FinOps catalog: schemas + operational control tables.
-- Idempotent: rendered with {catalog} and executed before every pipeline run.
-- Column lists here are the single source of truth aligned with the Python ops
-- modules (RunTracker, CheckpointStore, AlertOutbox, QualityEngine, publisher).

CREATE CATALOG IF NOT EXISTS {catalog};

CREATE SCHEMA IF NOT EXISTS {catalog}.config;
CREATE SCHEMA IF NOT EXISTS {catalog}.ops;
CREATE SCHEMA IF NOT EXISTS {catalog}.bronze;
CREATE SCHEMA IF NOT EXISTS {catalog}.silver;
CREATE SCHEMA IF NOT EXISTS {catalog}.quarantine;
CREATE SCHEMA IF NOT EXISTS {catalog}.audit;

-- Auditable effective-configuration snapshots (config versioning).
CREATE TABLE IF NOT EXISTS {catalog}.config.effective_config (
  run_id STRING, config_version STRING, config_hash STRING,
  snapshot_json STRING, created_at TIMESTAMP
);

-- One row per pipeline run.
CREATE TABLE IF NOT EXISTS {catalog}.ops.pipeline_run (
  run_id STRING, environment STRING, region STRING, workspace_id STRING,
  config_version STRING, config_hash STRING, status STRING,
  started_at TIMESTAMP, finished_at TIMESTAMP,
  sources_ok INT, sources_failed INT, sources_skipped INT
);

-- One row per source within a pipeline run (16 columns).
CREATE TABLE IF NOT EXISTS {catalog}.ops.source_run (
  run_id STRING, source_id STRING, workspace_id STRING, region STRING,
  lower_bound STRING, upper_bound STRING, status STRING,
  started_at TIMESTAMP, finished_at TIMESTAMP,
  rows_read BIGINT, rows_inserted BIGINT, rows_updated BIGINT, rows_quarantined BIGINT,
  retries INT, error_class STRING, error_message STRING
);

-- Append-versioned ingestion watermarks; advanced ONLY after commit+reconcile.
CREATE TABLE IF NOT EXISTS {catalog}.ops.source_checkpoint (
  source_id STRING, workspace_id STRING, region STRING, layer STRING,
  watermark_column STRING, watermark_value STRING, upper_bound STRING,
  last_run_id STRING, rows_processed BIGINT, committed_at TIMESTAMP, version BIGINT
);

-- Classified ingestion errors (retryable flag drives automatic self-healing).
CREATE TABLE IF NOT EXISTS {catalog}.ops.ingestion_error (
  run_id STRING, source_id STRING, error_class STRING, error_message STRING,
  retryable BOOLEAN, occurred_at TIMESTAMP
);

-- Human-approved recovery audit trail (no automatic destructive actions).
CREATE TABLE IF NOT EXISTS {catalog}.ops.recovery_action (
  action_id STRING, run_id STRING, source_id STRING, action STRING, reason STRING,
  outcome STRING, performed_by STRING, performed_at TIMESTAMP, requires_approval BOOLEAN
);

-- Observed schema fingerprints + drift summaries per source run.
CREATE TABLE IF NOT EXISTS {catalog}.ops.schema_registry (
  source_id STRING, fingerprint STRING, observed_at TIMESTAMP,
  drift_summary STRING, is_current BOOLEAN
);

-- Freshness SLO evaluation results.
CREATE TABLE IF NOT EXISTS {catalog}.ops.freshness_status (
  source_id STRING, latest_event_time TIMESTAMP, lag_minutes INT, status STRING,
  expected_interval_minutes INT, warning_after_minutes INT, critical_after_minutes INT,
  row_count BIGINT, evaluated_at TIMESTAMP
);

-- Cross-region transfer manifests with checksums (publish/consume audit).
CREATE TABLE IF NOT EXISTS {catalog}.ops.transfer_manifest (
  transfer_id STRING, source_region STRING, destination_region STRING,
  source_workspace_id STRING, dataset_name STRING, schema_version STRING,
  row_count BIGINT, min_event_time TIMESTAMP, max_event_time TIMESTAMP,
  payload_checksum STRING, published_at TIMESTAMP, consumed_at TIMESTAMP,
  transfer_status STRING, run_id STRING
);

-- Deduplicating alert outbox (reliable delivery).
CREATE TABLE IF NOT EXISTS {catalog}.ops.alert_outbox (
  alert_id STRING, alert_type STRING, severity STRING, message STRING,
  dedupe_key STRING, run_id STRING, source_id STRING,
  created_at TIMESTAMP, delivered_at TIMESTAMP, delivery_status STRING
);

-- Data-quality rule outcomes per run.
CREATE TABLE IF NOT EXISTS {catalog}.ops.data_quality_result (
  run_id STRING, source_id STRING, rule_id STRING, target_table STRING,
  evaluated_count BIGINT, failed_count BIGINT, action STRING, status STRING,
  evaluated_at TIMESTAMP
);
