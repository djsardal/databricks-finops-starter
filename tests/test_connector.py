"""Unit tests: incremental read-window resolution and watermark predicates."""
from datetime import datetime

from finops.connector import resolve_read_window, watermark_predicate

NOW = datetime(2026, 11, 25, 12, 0, 0)


def test_initial_load_reads_full_history():
    w = resolve_read_window("date", None, 72, now=NOW)
    assert w.is_initial_load
    assert w.lower_bound is None
    assert w.upper_bound == "2026-11-25"


def test_date_checkpoint_applies_lookback():
    w = resolve_read_window("date", "2026-11-20", 72, now=NOW)
    assert w.lower_bound == "2026-11-17"  # 72h lookback re-reads late arrivals
    assert w.upper_bound == "2026-11-25"


def test_timestamp_checkpoint_applies_lookback():
    w = resolve_read_window("timestamp", "2026-11-25 10:00:00", 2, now=NOW)
    assert w.lower_bound == "2026-11-25 08:00:00"
    assert w.upper_bound == "2026-11-25 12:00:00"


def test_watermark_predicate_bounds_date():
    w = resolve_read_window("date", "2026-11-20", 72, now=NOW)
    pred = watermark_predicate("usage_date", "date", w)
    assert "usage_date <= date '2026-11-25'" in pred
    assert "usage_date >= date '2026-11-17'" in pred


def test_initial_load_predicate_has_no_lower_bound():
    w = resolve_read_window("timestamp", None, 72, now=NOW)
    pred = watermark_predicate("start_time", "timestamp", w)
    assert "start_time >= " not in pred
    assert "start_time <= timestamp '2026-11-25 12:00:00'" in pred
