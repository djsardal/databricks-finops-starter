"""Generic Bronze writer: contract-driven DDL and idempotent MERGE.

Every source lands with the common metadata contract (spec section 4) plus the
full source row as a typed ``source_payload`` struct. The payload struct type
is derived from the LIVE source schema so nested fields are preserved exactly;
the MERGE populates it from the CONTRACT columns, so additive live columns
stay null until the contract is updated (no silent schema changes).

Identity: ``record_id = sha2(concat_ws('|', cloud, account_id, workspace_id,
region, source_system, source_table, source_record_id), 256)`` — see keys.py.
Merge matches on ``record_id`` (derived from the configured merge columns);
``WHEN MATCHED AND payload_hash changed THEN UPDATE`` makes replay and
late-arrival re-reads no-ops for unchanged records.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass

from .config import SourceConfig
from .connector import ReadWindow, watermark_predicate

log = logging.getLogger("finops.bronze")

COMMON_METADATA_DDL = """
  record_id           STRING NOT NULL,
  cloud               STRING,
  account_id          STRING,
  workspace_id        STRING,
  workspace_name      STRING,
  region              STRING,
  environment         STRING,
  source_system       STRING,
  source_table        STRING,
  source_record_id    STRING,
  source_event_time   TIMESTAMP,
  source_updated_time TIMESTAMP,
  source_ingested_at  TIMESTAMP,
  central_ingested_at TIMESTAMP,
  ingestion_run_id    STRING,
  config_version      STRING,
  schema_version      STRING,
  payload_hash        STRING,
  record_status       STRING,
  source_payload      {payload_type}
""".strip()


@dataclass
class MergeResult:
    rows_read: int
    rows_inserted: int
    rows_updated: int


class BronzeWriter:
    """Renders and executes Bronze DDL + MERGE for any configured source."""

    def __init__(self, spark, catalog: str):
        self.spark = spark
        self.catalog = catalog

    # ------------------------------------------------------------------ schema
    def _payload_struct_type(self, source: SourceConfig, contract_columns: dict[str, dict],
                             base_table: str | None = None) -> str:
        """Payload struct type from the LIVE source schema (exact nested types).

        For derived sources the type is the base Bronze table's payload struct.
        """
        if source.is_derived:
            if not base_table:
                raise ValueError("derived sources need base_table (bronze table of the base source)")
            base = self.spark.table(f"{self.catalog}.bronze.{base_table}")
            return base.schema["source_payload"].dataType.simpleString()
        live = self.spark.table(source.source_object)
        # Full struct<...> DDL type (matches the derived branch; the template's
        # `source_payload {payload_type}` expects the wrapped form).
        return live.schema.simpleString()


    def ensure_table(self, source: SourceConfig, contract_columns: dict[str, dict],
                     base_table: str | None = None) -> None:
        payload_type = self._payload_struct_type(source, contract_columns, base_table)
        ddl = COMMON_METADATA_DDL.replace("{payload_type}", payload_type)
        self.spark.sql(
            f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{source.target_schema}.{source.target_table} (
                {ddl}
                )"""
        )

    # ------------------------------------------------------------------- merge
    def _named_struct(self, columns: dict[str, dict], prefix: str = "") -> str:
        return "named_struct(" + ", ".join(
            f"'{name}', {prefix}{name}" for name in columns
        ) + ")"

    @staticmethod
    def _prefix_expr(expr: str, columns: dict[str, dict], prefix: str) -> str:
        """Prefix contract column references in an expression (derived sources)."""
        for name in columns:
            expr = re.sub(rf"\b{re.escape(name)}\b", f"{prefix}{name}", expr)
        return expr

    def render_merge(
        self,
        source: SourceConfig,
        contract_columns: dict[str, dict],
        ctx: dict,
        window: ReadWindow,
    ) -> str:
        """Build the full MERGE statement for one source run."""
        prefix = "source_payload." if source.is_derived else ""

        if source.is_derived:
            base_table = f"{self.catalog}.bronze.{ctx['base_table']}"
            pred = self._prefix_expr(source.derived_predicate, contract_columns, prefix)
            wm_col = f"{prefix}{source.watermark_column}"
            batch_from = f"SELECT source_payload FROM {base_table} WHERE {pred}"
            if window.lower_bound is not None:
                wtype = "date" if source.watermark_type == "date" else "timestamp"
                batch_from += (
                    f" AND {wm_col} >= {wtype} '{window.lower_bound}'"
                    f" AND {wm_col} <= {wtype} '{window.upper_bound}'"
                )
            batch_sql = f"SELECT * FROM ({batch_from})"
        else:
            pred = watermark_predicate(
                source.watermark_column, source.watermark_type, window
            ) if source.mode == "incremental" else "TRUE"
            batch_sql = (
                f"SELECT * FROM {source.source_object}"
                + (f" WHERE {pred}" if pred != "TRUE" else "")
            )

        srid = (
            source.source_record_id_column
            or source.source_record_id_expression
        )
        srid_sql = self._prefix_expr(srid, contract_columns, prefix) if source.is_derived else srid
        event_sql = (
            self._prefix_expr(source.event_time_column, contract_columns, prefix)
            if source.is_derived and source.event_time_column
            else (source.event_time_column or "NULL")
        )
        updated_sql = (
            self._prefix_expr(source.updated_time_column, contract_columns, prefix)
            if source.is_derived and source.updated_time_column
            else (source.updated_time_column or "NULL")
        )

        record_id_sql = (
            f"sha2(concat_ws('|', '{ctx['cloud']}', '{ctx['account_id']}', "
            f"'{ctx['workspace_id']}', '{ctx['region']}', '{ctx['source_system']}', "
            f"'{ctx['source_table']}', coalesce({srid_sql}, '')), 256)"
        )
        payload_sql = (
            "source_payload" if source.is_derived
            else self._named_struct(contract_columns)
        )

        # workspace_id: use the live column only when the contract carries it;
        # account-scoped sources (e.g. price lists) fall back to the configured
        # workspace identity.
        ws_id_sql = (
            f"coalesce({prefix}workspace_id, '{ctx['workspace_id']}')"
            if "workspace_id" in contract_columns
            else f"'{ctx['workspace_id']}'"
        )

        select_cols = f"""
            {record_id_sql} AS record_id,
            '{ctx['cloud']}' AS cloud,
            '{ctx['account_id']}' AS account_id,
            {ws_id_sql} AS workspace_id,
            '{ctx['workspace_name']}' AS workspace_name,
            '{ctx['region']}' AS region,
            '{ctx['environment']}' AS environment,
            '{ctx['source_system']}' AS source_system,
            '{ctx['source_table']}' AS source_table,
            cast({srid_sql} AS STRING) AS source_record_id,
            {event_sql} AS source_event_time,
            {updated_sql} AS source_updated_time,
            timestamp '{ctx['ingested_at']}' AS source_ingested_at,
            cast(NULL AS TIMESTAMP) AS central_ingested_at,
            '{ctx['run_id']}' AS ingestion_run_id,
            '{ctx['config_version']}' AS config_version,
            '{ctx['schema_version']}' AS schema_version,
            sha2(to_json({payload_sql}), 256) AS payload_hash,
            'valid' AS record_status,
            {payload_sql} AS source_payload"""

        target = f"{self.catalog}.{source.target_schema}.{source.target_table}"
        return f"""
            MERGE INTO {target} AS t
            USING (
              WITH batch AS ({batch_sql})
              SELECT {select_cols}
              FROM batch
            ) AS s
            ON t.record_id = s.record_id
            WHEN MATCHED AND t.payload_hash <> s.payload_hash THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *"""

    def write(
        self,
        source: SourceConfig,
        contract_columns: dict[str, dict],
        ctx: dict,
        window: ReadWindow,
    ) -> MergeResult:
        """Ensure the table, count the batch, execute the merge, read metrics."""
        self.ensure_table(source, contract_columns, base_table=ctx.get("base_table"))
        target = f"{self.catalog}.{source.target_schema}.{source.target_table}"

        if source.is_derived:
            prefix = "source_payload."
            pred = self._prefix_expr(source.derived_predicate, contract_columns, prefix)
            wm_col = f"{prefix}{source.watermark_column}"
            base_table = f"{self.catalog}.bronze.{ctx['base_table']}"
            count_sql = f"SELECT count(*) AS n FROM {base_table} WHERE {pred}"
            if window.lower_bound is not None:
                wtype = "date" if source.watermark_type == "date" else "timestamp"
                count_sql += (
                    f" AND {wm_col} >= {wtype} '{window.lower_bound}'"
                    f" AND {wm_col} <= {wtype} '{window.upper_bound}'"
                )
        else:
            pred = watermark_predicate(
                source.watermark_column, source.watermark_type, window
            ) if source.mode == "incremental" else "TRUE"
            count_sql = (
                f"SELECT count(*) AS n FROM {source.source_object}"
                + (f" WHERE {pred}" if pred != "TRUE" else "")
            )

        rows_read = int(
            self.spark.sql(count_sql).collect()[0].n
        )

        merge_sql = self.render_merge(source, contract_columns, ctx, window)
        self.spark.sql(merge_sql)

        history = self.spark.sql(
            f"DESCRIBE HISTORY {target} LIMIT 1"
        ).collect()
        op = history[0].asDict() if history else {}
        inserted = int(op.get("numTargetRowsInserted") or 0)
        updated = int(op.get("numTargetRowsUpdated") or 0)
        log.info(
            "bronze merge %s: read=%d inserted=%d updated=%d",
            source.source_id, rows_read, inserted, updated,
        )
        return MergeResult(rows_read, inserted, updated)
