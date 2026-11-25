"""Checkpoint store backed by ``ops.source_checkpoint``.

One checkpoint per (source_id, workspace_id, region, layer). A checkpoint row
advances ONLY after the corresponding target commit and reconciliation have
succeeded — the pipeline calls :meth:`commit` after the Bronze or Silver merge
completes. Checkpoint rows are append-versioned (history preserved) via merge
on the natural key.

Resetting or recreating a checkpoint is a destructive, approval-gated action
(see docs/operations_runbook.md); this module offers no automatic reset.
"""
from __future__ import annotations

import logging
from typing import Optional

log = logging.getLogger("finops.checkpoint")


class CheckpointStore:
    """Reads and commits per-source watermarks in ``ops.source_checkpoint``."""

    def __init__(self, spark, catalog: str, workspace_id: str, region: str):
        self.spark = spark
        self.catalog = catalog
        self.workspace_id = workspace_id
        self.region = region
        self.table = f"{catalog}.ops.source_checkpoint"

    def ensure_table(self) -> None:
        self.spark.sql(
            f"""CREATE TABLE IF NOT EXISTS {self.table} (
                  source_id        STRING NOT NULL,
                  workspace_id     STRING NOT NULL,
                  region           STRING NOT NULL,
                  layer            STRING NOT NULL,
                  watermark_column STRING,
                  watermark_value  STRING,
                  upper_bound      STRING,
                  last_run_id      STRING,
                  rows_processed   BIGINT,
                  committed_at     TIMESTAMP,
                  version          BIGINT
                )"""
        )

    def get(self, source_id: str, layer: str) -> Optional[str]:
        """Latest committed watermark value for a source/layer, or None."""
        rows = self.spark.sql(
            f"""SELECT watermark_value FROM {self.table}
                WHERE source_id = '{source_id}' AND layer = '{layer}'
                  AND workspace_id = '{self.workspace_id}' AND region = '{self.region}'
                ORDER BY version DESC LIMIT 1"""
        ).collect()
        return rows[0].watermark_value if rows else None

    def commit(
        self,
        source_id: str,
        layer: str,
        watermark_column: str,
        watermark_value: str,
        upper_bound: str,
        run_id: str,
        rows_processed: int,
    ) -> None:
        """Persist an advanced checkpoint (call only after commit+reconcile succeed)."""
        version_rows = self.spark.sql(
            f"""SELECT coalesce(max(version), 0) AS v FROM {self.table}
                WHERE source_id = '{source_id}' AND layer = '{layer}'
                  AND workspace_id = '{self.workspace_id}' AND region = '{self.region}'"""
        ).collect()
        version = int(version_rows[0].v) + 1
        self.spark.sql(
            f"""INSERT INTO {self.table} VALUES
                ('{source_id}', '{self.workspace_id}', '{self.region}', '{layer}',
                 '{watermark_column}', '{watermark_value}', '{upper_bound}',
                 '{run_id}', {rows_processed}, current_timestamp(), {version})"""
        )
        log.info(
            "checkpoint committed: source=%s layer=%s value=%s run=%s (v%d)",
            source_id, layer, watermark_value, run_id, version,
        )
