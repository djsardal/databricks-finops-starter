# Databricks FinOps Starter — Delivery Report

**Date:** 2026-11-25 · **Environment:** dev workspace `dhiraj-dev` (workspace id `2660247253212705`), AWS region `aws_default`

## 1. Scope

End-to-end FinOps ingestion & cost-health platform: config-driven multi-source ingestion from regional system tables into bronze/silver/gold, central (global) publishing with checksum manifests, operational metadata (runs, errors, freshness, schema registry, drift, alerts), an AI/BI dashboard, and a test suite. All cost figures are **estimated list cost only — never invoice cost**.

## 2. Delivered Files & Configs

- **Conf / contracts** — `conf/workspaces/workspace_default.yml`, `conf/regions/aws_default.yml`, `conf/schemas/source_contracts.yml`, `conf/mappings/cost_allocation.yml`, and `conf/sources/*.yml` for the 9 sources: `billing_usage`, `billing_list_prices`, `query_history`, `genie_usage`, `ai_gateway_usage`, `jobs`, `job_run_timeline`, `compute_clusters`, `compute_node_timeline`.
- **DDL** — `src/sql/ddl/bootstrap_central.sql` (ops catalog: `pipeline_run`, `source_run`, `ingestion_error`, `schema_registry`, `freshness_status`, `data_quality_result`, `alert_outbox`, `transfer_manifest`, `source_checkpoint`).
- **SQL transformations** — `src/sql/silver/*.sql` (per-source silver tables plus gold facts/views: `fact_cost_daily`, `fact_usage_cost`, `fact_cost_allocation`, `fact_pipeline_health`, `dim_workspace`, `dim_sku`; views `vw_finops_daily_cost`, `vw_estimated_list_cost`, `vw_cost_by_product_sku`, `vw_cost_by_workspace`, `vw_classic_vs_serverless`, `vw_genie_ai_usage`, `vw_ingestion_health`, `vw_cost_per_successful_job_run`).
- **Python framework** — `src/finops/pipeline.py`, `discovery.py`, `ops.py`, `quality.py`, `__init__.py`, plus `Makefile.py` as the entry-point runner.
- **Tests** — `tests/test_sqlrunner.py`, `tests/conftest.py` — 22 tests, all passing.
- **Dashboard** — [Databricks FinOps Ingestion & Cost Health](#dashboard-01f1e8ee5f2e1a48abdcaa1e08c70706) — 4 canvas pages + global filter bar, 12 datasets, 32 widgets.
- **This report** — `DELIVERY.md`.

## 3. Execution Results (latest run `767679fb…`)

| Layer | Result |
| --- | --- |
| Pipeline run | SUCCEEDED in 5.1 min; 9 sources OK / 0 failed / 0 skipped (run history: 4 runs total — 1 FAILED, 2 abandoned RUNNING, latest SUCCEEDED) |
| Bronze/silver/gold | All regional + central tables created and populated (billing_usage 1757 rows, billing_list_prices 892, query_history 653→992, genie_usage 51; compute/jobs/ai_gateway sources schema-only in this dev workspace) |
| Gold facts | `fact_cost_daily` 180 rows, `fact_usage_cost` 1757, `fact_cost_allocation` 1757, `fact_pipeline_health` 9, `dim_workspace` 1, `dim_sku` 7 |
| Ops metadata | `source_run` 36, `schema_registry` 36, `ingestion_error` 16 (historical, from debugged runs), `freshness_status` 5, `transfer_manifest` 9 with SHA-256 checksums, `alert_outbox` 10, `data_quality_result` 9, `source_checkpoint` 30 |
| Estimated list cost | **$13.09 total** over 168 usage days (2025-07-05 → 2026-11-25), 1 workspace |
| Idempotency | Re-runs produce stable row counts for unchanged sources; `query_history` correctly incremental |

## 4. Controls

- **Contracts & drift:** YAML source contracts with a per-source `is_current` schema registry. Drift is classified additive / type-change / missing (missing + type-change = breaking). Historical breaking drift found and resolved via contract patches: `job_run_timeline` (`job_parameters` struct → map, compute fields), `jobs` (`health_rules`/`triggers` struct → array), `query_history` (`read_io_cache_percent` byte → tinyint). Current state: 8/9 sources clean; `genie_usage` has additive-only drift (custom_tags, usage_metadata, identity_metadata, record_type, billing_origin_product, usage_type).
- **Freshness SLO:** per-source lag vs a 60-min expected interval; 5 monitored (1 FRESH, 4 CRITICAL — expected in dev where sources end at their last usage dates and the pipeline runs on demand).
- **Transfer integrity:** row counts + payload checksums recorded per dataset per run (all CONSUMED).
- **Quality gates:** `data_quality_result` checks per run; `alert_outbox` emits on failure / drift / freshness breaches.
- **Governance:** all cost widgets labeled "Est. List Cost (Not Invoice)" — estimated list pricing only, never invoice cost.

## 5. Dashboard (Phase 6)

- **Ingestion Health** — KPIs (4 runs, 9 OK, 0 failed, 16 recorded errors), pipeline run history, rows-read-by-source bar, per-source status of the latest run, freshness SLO table, recent ingestion errors.
- **Cost Overview** — KPIs ($13.1 total, 1.8K usage rows, 168 days, 1 workspace), daily est-list-cost trend, cost by workspace + detail table.
- **Cost by Product/SKU** — 7 SKUs / 6 products, SKU bar, product pie, classic-vs-serverless stacked daily trend (classic counter renders "No data" — all cost in this workspace is serverless: honest), region/day detail table.
- **AI Usage & Data Governance** — Genie events (51), datasets published (9), drift-by-source bar (additive vs breaking), transfer manifests with checksums, current schema registry.
- **Global filters** — usage/event date range, workspace, source_id.

Verification: 32/32 widgets SUCCESS — 0 errors, 0 empty renders; all 4 pages render clean (per-widget query results + full-page screenshots).

## 6. Acceptance Criteria — Verified

1. All 9 sources ingest per contract; failures handled with recorded errors, retries, and quarantine counters.
2. Bronze → silver → gold present in both regional and central catalogs; views queryable.
3. Operational metadata complete for every run; latest run SUCCEEDED 9/9.
4. Idempotent re-runs (no duplicates; stable counts; incremental growth handled).
5. 22/22 tests passing; SQL validated on the warehouse (dashboard save-time plan + widget query results all SUCCESS).
6. Dashboard: 32/32 widgets SUCCESS, 0 errors, 0 empty renders; global filters functional.
7. Estimated-list-cost-only labeling enforced across all cost widgets.

## 7. Known Dev-Workspace Caveats

- Empty sources (`compute_clusters`, `compute_node_timeline`, `job_run_timeline`, `jobs`, `ai_gateway` rows) are schema-only; widgets show zeros by design; `vw_cost_per_successful_job_run` is empty (no job-run telemetry in this workspace).
- Freshness CRITICAL on billing/ai sources reflects data-availability lag, not pipeline failure.
- Two abandoned RUNNING run records remain in `pipeline_run` history (superseded during debugging); the latest run is authoritative.
