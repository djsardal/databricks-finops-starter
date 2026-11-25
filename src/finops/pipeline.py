"""End-to-end pipeline orchestration.

Lifecycle per source (spec section 6): discover -> validate -> read only the
configured incremental range -> land in regional Bronze -> reconcile counts ->
normalize into Silver -> quality rules -> publish approved datasets ->
checkpoints/metrics/errors -> alerts. Notebooks stay thin and call
:func:`run_pipeline`.

Failure isolation: a failing source never blocks unrelated sources. Required
sources fail loudly; optional sources are marked SKIPPED_OPTIONAL. Only known
transient failures are retried; destructive recovery always requires human
approval (docs/operations_runbook.md).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

from .bronze import BronzeWriter
from .checkpoint import CheckpointStore
from .config import EffectiveConfig
from .connector import resolve_read_window
from .discovery import SourceInspector
from .ops import (
    AlertOutbox,
    CircuitBreaker,
    ErrorCategory,
    FreshnessMonitor,
    RetryPolicy,
    RunTracker,
    classify_exception,
)
from .quality import DataQualityError, QualityEngine
from .sqlrunner import run_sql_file
from .transfer import LocalCatalogConsumer, LocalCatalogPublisher

log = logging.getLogger("finops.pipeline")

# Source -> reusable Silver SQL transformation (relative to src/sql).
SILVER_SQL = {
    "billing_usage": "silver/billing_usage.sql",
    "billing_list_prices": "silver/billing_list_prices.sql",
    "query_history": "silver/query_history.sql",
    "genie_usage": "silver/genie_usage.sql",
    "ai_gateway_usage": "silver/ai_gateway_usage.sql",
}
# Source -> silver fact table (for quality rules and freshness).
SILVER_TABLE = {
    "billing_usage": "fact_usage",
    "billing_list_prices": "fact_price",
    "query_history": "fact_query_execution",
    "genie_usage": "fact_ai_usage",
    "ai_gateway_usage": "fact_ai_usage",
}


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat(sep=" ")


def bootstrap_catalogs(spark, cfg: EffectiveConfig, sql_root: Path) -> None:
    """Idempotent catalog/schema/ops-table creation for regional + central catalogs."""
    run_sql_file(spark, sql_root / "ddl" / "bootstrap_regional.sql", catalog=cfg.regional_catalog)
    if cfg.deployment_mode == "single_workspace" or cfg.region.get("central_transfer", {}).get("enabled"):
        run_sql_file(spark, sql_root / "ddl" / "bootstrap_central.sql", catalog=cfg.central_catalog)


def _record_schema(spark, catalog: str, source_id: str, fingerprint: str, drift_summary: str) -> None:
    spark.sql(
        f"""UPDATE {catalog}.ops.schema_registry SET is_current = false
            WHERE source_id = '{source_id}'"""
    )
    spark.sql(
        f"""INSERT INTO {catalog}.ops.schema_registry
            VALUES ('{source_id}', '{fingerprint}', current_timestamp(),
                    '{drift_summary.replace(chr(39), chr(39)*2)[:2000]}', true)"""
    )


def _record_error(spark, catalog: str, run_id: str, source_id: str,
                  category: ErrorCategory, retryable: bool, message: str) -> None:
    spark.sql(
        f"""INSERT INTO {catalog}.ops.ingestion_error
            VALUES ('{run_id}', '{source_id}', '{category.value}',
                    '{message.replace(chr(39), chr(39)*2)[:2000]}', {str(retryable).lower()},
                    current_timestamp())"""
    )


def run_pipeline(
    spark,
    conf_root: str | Path,
    environment: str,
    region_id: str | None = None,
    workspace_id: str | None = None,
    sources: list[str] | None = None,
    run_id: str | None = None,
    build_gold: bool = True,
) -> dict:
    """Run the full ingestion lifecycle for one environment/region/workspace."""
    repo_root = Path(__file__).resolve().parents[2]
    sql_root = repo_root / "src" / "sql"
    conf_root = Path(conf_root)

    cfg = EffectiveConfig.load(conf_root, environment, region_id, workspace_id)
    log.info(
        "config loaded: env=%s region=%s catalog=%s hash=%s",
        cfg.environment, cfg.region["region_id"], cfg.regional_catalog, cfg.effective_hash[:12],
    )

    bootstrap_catalogs(spark, cfg, sql_root)

    tracker = RunTracker(spark, cfg.regional_catalog)
    checkpoints = CheckpointStore(
        spark, cfg.regional_catalog,
        str(cfg.workspace["workspace_id"]), cfg.region["region_id"],
    )
    checkpoints.ensure_table()
    inspector = SourceInspector(spark)
    writer = BronzeWriter(spark, cfg.regional_catalog)
    breaker = CircuitBreaker(
        spark, cfg.regional_catalog,
        threshold=int(cfg.reliability.get("max_consecutive_failures", 5)),
    )
    alerts = AlertOutbox(spark, cfg.regional_catalog)
    quality = QualityEngine(
        spark, cfg.regional_catalog, cfg.quality_rules,
        max_quarantine_pct=float(cfg.reliability.get("max_quarantine_percentage", 1.0)),
    )
    quality.ensure_tables()
    retry_policy = RetryPolicy(
        max_attempts=int(cfg.ingestion.get("default_retry_count", 3)),
        backoff_seconds=tuple(cfg.ingestion.get("retry_backoff_seconds", (30, 120, 300))),
    )

    run_id = run_id or tracker.new_run_id()
    ws_id = str(cfg.workspace["workspace_id"])
    region = cfg.region["region_id"]
    tracker.start_pipeline(
        run_id, cfg.environment, region, ws_id,
        cfg.config_version, cfg.effective_hash,
    )
    # Effective-config snapshot (auditable config versioning)
    spark.sql(
        f"""INSERT INTO {cfg.regional_catalog}.config.effective_config
            VALUES ('{run_id}', '{cfg.config_version}', '{cfg.effective_hash}',
                    '""" + _snapshot_json(cfg) + """', current_timestamp())"""
    )

    # Base sources first, then derived sources (their bronze depends on the base).
    ordered = [s for s in cfg.enabled_sources if not s.is_derived] + \
              [s for s in cfg.enabled_sources if s.is_derived]
    if sources:
        wanted = set(sources)
        ordered = [s for s in ordered if s.source_id in wanted]

    results: dict[str, dict] = {}
    counts = {"ok": 0, "failed": 0, "skipped": 0}
    required_failed = False

    for src in ordered:
        sid = src.source_id
        tracker.start_source(run_id, sid, ws_id, region, "", "")
        result: dict = {"source_id": sid}

        # ---- circuit breaker --------------------------------------------------
        allowed, failures = breaker.allow(sid, ws_id, region)
        if not allowed:
            result["status"] = "SKIPPED_BREAKER"
            counts["skipped"] += 1
            tracker.finish_source(run_id, sid, "SKIPPED_BREAKER")
            alerts.emit(
                "circuit_open", "critical",
                f"source {sid} paused after {failures} consecutive failures",
                f"breaker_open:{sid}", run_id, sid,
            )
            results[sid] = result
            continue

        try:
            # ---- discovery + drift -------------------------------------------
            contract = cfg.contracts[src.contract_ref]
            inspection = inspector.inspect(src.source_object)
            if not inspection.exists or not inspection.readable:
                category, _ = classify_exception(
                    RuntimeError(inspection.probe_error or "source table missing")
                )
                if src.required:
                    required_failed = True
                    status = "FAILED"
                    counts["failed"] += 1
                else:
                    status = "SKIPPED_OPTIONAL"
                    counts["skipped"] += 1
                _record_error(
                    spark, cfg.regional_catalog, run_id, sid, ErrorCategory.SOURCE_MISSING,
                    False, inspection.probe_error or f"{src.source_object} not found",
                )
                tracker.finish_source(
                    run_id, sid, status,
                    error_class=ErrorCategory.SOURCE_MISSING.value,
                    error_message=inspection.probe_error or "source missing",
                )
                if status == "FAILED":
                    alerts.emit("source_missing", "critical",
                                f"required source {sid} unavailable",
                                f"missing:{sid}", run_id, sid)
                result["status"] = status
                results[sid] = result
                continue

            drift = inspector.compare_with_contract(
                sid, inspection, contract.get("columns") or {},
                contract.get("required_columns") or [],
            )
            drift_summary = (
                f"missing={drift.missing_required}; additive={drift.additive_columns}; "
                f"type_changes={drift.type_changes}"
            )
            _record_schema(spark, cfg.regional_catalog, sid, drift.fingerprint, drift_summary)
            if drift.is_breaking:
                if src.required:
                    required_failed = True
                    counts["failed"] += 1
                    status = "FAILED"
                else:
                    status = "SKIPPED_OPTIONAL"
                    counts["skipped"] += 1
                _record_error(
                    spark, cfg.regional_catalog, run_id, sid, ErrorCategory.SCHEMA_DRIFT,
                    False, f"breaking drift: {drift_summary}",
                )
                tracker.finish_source(
                    run_id, sid, status, error_class=ErrorCategory.SCHEMA_DRIFT.value,
                    error_message=drift_summary[:500],
                )
                alerts.emit("schema_drift", "critical",
                            f"breaking schema drift on {sid}: {drift_summary}",
                            f"drift:{sid}", run_id, sid)
                result.update(status=status, drift=drift_summary)
                results[sid] = result
                continue
            if not drift.is_clean:
                # additive drift: capture and continue (capture_and_review)
                alerts.emit("schema_drift", "warning",
                            f"additive schema drift on {sid}: {drift_summary}",
                            f"drift_add:{sid}", run_id, sid)
            result["drift"] = drift_summary

            # ---- bronze write -------------------------------------------------
            ckpt = checkpoints.get(sid, "bronze")
            window = resolve_read_window(
                src.watermark_type or "timestamp", ckpt, src.lookback_hours
            )
            ctx = {
                "cloud": cfg.platform["cloud"],
                "account_id": cfg.workspace["account_id"],
                "workspace_id": ws_id,
                "workspace_name": cfg.workspace.get("workspace_name", ""),
                "region": region,
                "environment": cfg.environment,
                "source_system": "system_table" if not src.is_derived else "derived_system_table",
                "source_table": src.source_object,
                "run_id": run_id,
                "config_version": cfg.config_version,
                "schema_version": "1.0.0",
                "ingested_at": _utcnow_iso(),
                "base_table": cfg.sources[src.derived_from].target_table if src.is_derived else None,
            }

            merge = retry_policy.with_retry(
                lambda: writer.write(src, contract.get("columns") or {}, ctx, window)
            )
            result.update(rows_read=merge.rows_read, rows_inserted=merge.rows_inserted,
                          rows_updated=merge.rows_updated)

            # reconciliation: rows landed can never exceed rows read
            if merge.rows_inserted + merge.rows_updated > merge.rows_read:
                raise RuntimeError(
                    f"reconciliation failure: landed {merge.rows_inserted + merge.rows_updated} "
                    f"vs read {merge.rows_read}"
                )

            # ---- silver normalization -----------------------------------------
            silver_table = SILVER_TABLE.get(sid)
            if sid in SILVER_SQL:
                silver_lower = checkpoints.get(sid, "silver") or "1970-01-01 00:00:00"
                run_sql_file(
                    spark, sql_root / SILVER_SQL[sid],
                    catalog=cfg.regional_catalog, silver_lower=silver_lower,
                )
                checkpoints.commit(
                    sid, "silver", "source_ingested_at", ctx["ingested_at"],
                    window.upper_bound, run_id, merge.rows_read,
                )

            # ---- data quality --------------------------------------------------
            quarantined = 0
            if silver_table:
                try:
                    rule_results = quality.evaluate(sid, silver_table, run_id)
                    quarantined = sum(
                        r.failed for r in rule_results if r.action == "quarantine"
                    )
                    result["quality"] = {
                        r.rule_id: {"failed": r.failed, "status": r.status}
                        for r in rule_results
                    }
                except DataQualityError as exc:
                    raise RuntimeError(f"data quality tolerance exceeded: {exc}") from exc

            # ---- checkpoint + run completion ---------------------------------
            checkpoints.commit(
                sid, "bronze", src.watermark_column or "snapshot",
                window.upper_bound, window.upper_bound, run_id, merge.rows_read,
            )
            tracker.finish_source(
                run_id, sid, "SUCCEEDED",
                rows_read=merge.rows_read, rows_inserted=merge.rows_inserted,
                rows_updated=merge.rows_updated, rows_quarantined=quarantined,
            )
            counts["ok"] += 1
            result["status"] = "SUCCEEDED"

        except Exception as exc:  # noqa: BLE001 - one source must not block others
            category, retryable = classify_exception(exc)
            _record_error(
                spark, cfg.regional_catalog, run_id, sid, category, retryable,
                f"{type(exc).__name__}: {exc}",
            )
            if src.required:
                required_failed = True
                status = "FAILED"
                counts["failed"] += 1
            else:
                status = "SKIPPED_OPTIONAL"
                counts["skipped"] += 1
            tracker.finish_source(
                run_id, sid, status, error_class=category.value,
                error_message=f"{type(exc).__name__}: {exc}"[:500],
            )
            alerts.emit(
                "source_failed", "critical" if src.required else "warning",
                f"source {sid} failed: {type(exc).__name__}: {str(exc)[:300]}",
                f"failed:{sid}", run_id, sid,
            )
            result["status"] = status
            result["error"] = f"{category.value}: {str(exc)[:300]}"
            log.exception("source %s failed", sid)
        results[sid] = result

    # Silver event-time column per dataset (manifest bounds + freshness).
    SILVER_EVENT_COL = {
        "billing_usage": "usage_start_time",
        "billing_list_prices": "price_start_time",
        "query_history": "start_time",
        "genie_usage": "event_time",
        "ai_gateway_usage": "event_time",
    }

    # ---- central publish (Phase 5, local adapter in single_workspace mode) ----
    publish_manifests = []
    if cfg.deployment_mode == "single_workspace" or cfg.region.get("central_transfer", {}).get("enabled"):
        publisher = LocalCatalogPublisher(
            spark, cfg.regional_catalog, cfg.central_catalog,
            source_region=region, destination_region="global",
            workspace_id=ws_id, schema_version=cfg.config_version,
        )
        consumer = LocalCatalogConsumer(spark, cfg.regional_catalog, cfg.central_catalog)
        for dataset, silver in sorted(SILVER_TABLE.items()):
            if results.get(dataset, {}).get("status") not in {"SUCCEEDED"}:
                continue
            manifest = publisher.publish(
                dataset, silver, silver, run_id,
                event_col=SILVER_EVENT_COL.get(dataset, "source_ingested_at"),
            )
            consumer.consume(manifest, silver, silver, run_id)
            publish_manifests.append(
                {"dataset": dataset, "rows": manifest.row_count,
                 "checksum": manifest.payload_checksum[:12]}
            )

    # ---- gold build (Phase 4) -------------------------------------------------
    if build_gold and not required_failed:
        run_sql_file(
            spark, sql_root / "gold" / "gold_model.sql",
            central_catalog=cfg.central_catalog,
            regional_catalog=cfg.regional_catalog,
            allocation_version=cfg.allocation.get("allocation_version", "1.0.0"),
        )
        run_sql_file(
            spark, sql_root / "gold" / "gold_views.sql",
            central_catalog=cfg.central_catalog,
            regional_catalog=cfg.regional_catalog,
        )

    # ---- freshness --------------------------------------------------------------
    slo = cfg.freshness_slo
    freshness = FreshnessMonitor(spark, cfg.regional_catalog)
    for sid, silver_table in SILVER_TABLE.items():
        if sid in results:
            event_col = SILVER_EVENT_COL.get(sid, "event_time")
            freshness.evaluate(
                sid, silver_table, event_col,
                int(slo.get("expected_interval_minutes", 60)),
                int(slo.get("warning_after_minutes", 90)),
                int(slo.get("critical_after_minutes", 180)),
            )

    status = "FAILED" if required_failed else (
        "SUCCEEDED" if counts["failed"] == 0 else "SUCCEEDED_WITH_ERRORS"
    )
    tracker.finish_pipeline(
        run_id, status, counts["ok"], counts["failed"], counts["skipped"]
    )

    return {
        "run_id": run_id,
        "status": status,
        "regional_catalog": cfg.regional_catalog,
        "central_catalog": cfg.central_catalog,
        "config_version": cfg.config_version,
        "config_hash": cfg.effective_hash,
        "counts": counts,
        "sources": results,
        "publish_manifests": publish_manifests,
    }


def _snapshot_json(cfg: EffectiveConfig) -> str:
    import json

    payload = json.dumps(cfg.to_dict(), sort_keys=True, default=str)
    return payload.replace("'", "''")
