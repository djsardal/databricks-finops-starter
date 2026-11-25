"""Source discovery, schema fingerprinting and drift detection.

Inspects live System Tables (existence, columns/types, readability) WITHOUT
writing anything, and compares the observed top-level schema with the declared
data contract. Distinguishes:

* missing table            -> SOURCE_MISSING (permission errors are surfaced, not assumed)
* permission denied        -> PERMISSION (probe error text)
* missing required columns -> breaking drift (fail_source)
* unexpected type change  -> breaking drift (quarantine_and_alert)
* additive columns        -> additive drift (capture_and_review, ingestion continues
  on contract columns; the new column lands after the contract is updated)

Struct-typed columns are compared at the top level only — nested struct drift
is additive by design because the whole struct is landed in the Bronze payload.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

from .keys import sha256_hex

log = logging.getLogger("finops.discovery")


def _norm_type(t: str) -> str:
    t = str(t).strip().lower()
    if t.startswith("struct"):
        return "struct"
    if t.startswith("array"):
        return "array"
    if t.startswith("map"):
        return t  # keep full map<...> signature
    return t


@dataclass
class SourceInspection:
    """Observed state of one source object."""

    source_object: str
    exists: bool
    readable: bool
    columns: list[tuple[str, str]] = field(default_factory=list)  # (name, declared simple type)
    probe_error: str | None = None

    @property
    def fingerprint(self) -> str:
        """Stable SHA-256 over the sorted 'name:type' pairs."""
        return sha256_hex(
            ";".join(f"{n}:{_norm_type(t)}" for n, t in sorted(self.columns))
        )


@dataclass
class DriftReport:
    """Contract comparison outcome for one source."""

    source_id: str
    fingerprint: str
    missing_required: list[str] = field(default_factory=list)
    additive_columns: list[str] = field(default_factory=list)
    type_changes: list[tuple[str, str, str]] = field(default_factory=list)  # (col, expected, actual)

    @property
    def is_breaking(self) -> bool:
        return bool(self.missing_required or self.type_changes)

    @property
    def is_clean(self) -> bool:
        return not (self.missing_required or self.additive_columns or self.type_changes)


class SourceInspector:
    """Inspects system tables via information_schema + DESCRIBE + probes."""

    def __init__(self, spark):
        self.spark = spark

    def inspect(self, source_object: str) -> SourceInspection:
        catalog, schema, table = self._parse(source_object)
        rows = self.spark.sql(
            "SELECT table_name FROM system.information_schema.tables "
            f"WHERE table_catalog = '{catalog}' AND table_schema = '{schema}' "
            f"  AND table_name = '{table}'"
        ).collect()
        if not rows:
            return SourceInspection(source_object, exists=False, readable=False)

        try:
            desc = self.spark.sql(f"DESCRIBE TABLE {source_object}").collect()
            columns = [(r["col_name"], r["data_type"]) for r in desc if not r["col_name"].startswith("#")]
        except Exception as exc:  # pragma: no cover - permission path
            return SourceInspection(
                source_object, exists=True, readable=False, probe_error=str(exc)[:300]
            )

        readable, probe_error = True, None
        try:
            self.spark.sql(f"SELECT 1 FROM {source_object} LIMIT 1").collect()
        except Exception as exc:
            readable, probe_error = False, str(exc)[:300]

        return SourceInspection(
            source_object, exists=True, readable=readable,
            columns=columns, probe_error=probe_error,
        )

    def compare_with_contract(
        self,
        source_id: str,
        inspection: SourceInspection,
        contract_columns: dict[str, dict],
        required_columns: list[str],
    ) -> DriftReport:
        """Compare observed top-level schema with the declared contract."""
        observed = {name: _norm_type(dtype) for name, dtype in inspection.columns}
        expected = {name: _norm_type(spec["type"]) for name, spec in contract_columns.items()}

        missing_required = [c for c in required_columns if c not in observed]
        additive = [c for c in observed if c not in expected]
        type_changes = [
            (c, expected[c], observed[c])
            for c in expected
            if c in observed and expected[c] != observed[c]
        ]
        return DriftReport(
            source_id=source_id,
            fingerprint=inspection.fingerprint,
            missing_required=missing_required,
            additive_columns=additive,
            type_changes=type_changes,
        )

    @staticmethod
    def _parse(source_object: str) -> tuple[str, str, str]:
        parts = source_object.split(".")
        if len(parts) != 3:
            raise ValueError(f"source_object must be catalog.schema.table, got {source_object}")
        return parts[0], parts[1], parts[2]
