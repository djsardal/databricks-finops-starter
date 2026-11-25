"""Source read adapters — read-only against System Tables.

The connector never writes to a source. It builds the incremental read
predicate (watermark column + bounded lookback) and provides cheap probes used
by discovery and reconciliation. The actual batch read happens inside the
Bronze MERGE statement (single source of truth), so the predicate returned
here is embedded into that SQL.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional


@dataclass(frozen=True)
class ReadWindow:
    """Resolved incremental read window."""

    lower_bound: Optional[str]  # ISO date or timestamp string, None = full history
    upper_bound: str            # ISO date or timestamp string
    lookback_hours: int

    @property
    def is_initial_load(self) -> bool:
        return self.lower_bound is None


def resolve_read_window(
    watermark_type: str,
    checkpoint_value: Optional[str],
    lookback_hours: int,
    now: Optional[datetime] = None,
) -> ReadWindow:
    """Compute the lower/upper read bounds for one source run.

    * No checkpoint            -> full history (initial load / backfill).
    * Checkpoint + lookback    -> bounded re-read window so late-arriving and
      restated records within the lookback horizon are merged again. Replay of
      the same window is a no-op because the Bronze merge matches on record_id
      and only updates rows whose payload hash changed.
    """
    now = now or datetime.utcnow()
    if watermark_type == "date":
        upper = now.date().isoformat()
        if checkpoint_value is None:
            return ReadWindow(None, upper, lookback_hours)
        lower_dt = date.fromisoformat(checkpoint_value) - timedelta(hours=lookback_hours)
        return ReadWindow(lower_dt.isoformat(), upper, lookback_hours)
    # timestamp watermark
    upper = now.replace(microsecond=0).isoformat(sep=" ")
    if checkpoint_value is None:
        return ReadWindow(None, upper, lookback_hours)
    lower_dt = datetime.fromisoformat(checkpoint_value) - timedelta(hours=lookback_hours)
    return ReadWindow(lower_dt.replace(microsecond=0).isoformat(sep=" "), upper, lookback_hours)


def watermark_predicate(
    watermark_column: str,
    watermark_type: str,
    window: ReadWindow,
) -> str:
    """SQL predicate selecting the incremental batch from the source."""
    if watermark_type == "date":
        upper = f"date '{window.upper_bound}'"
        lower = None if window.lower_bound is None else f"date '{window.lower_bound}'"
    else:
        upper = f"timestamp '{window.upper_bound}'"
        lower = None if window.lower_bound is None else f"timestamp '{window.lower_bound}'"
    parts = [f"{watermark_column} <= {upper}"]
    if lower is not None:
        parts.append(f"{watermark_column} >= {lower}")
    return " AND ".join(parts)
