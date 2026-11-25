"""Operational framework: run tracking, error classification, bounded retry with
exponential backoff and jitter, per-source circuit breaker, freshness monitoring
and a deduplicating alert outbox.

Self-healing is bounded and auditable: only known-transient failures are retried
automatically. Destructive actions (checkpoint resets, historical deletion,
retention/residency changes, breaking schema acceptance) always require human
approval and are NOT implemented here — see docs/operations_runbook.md.
"""
from __future__ import annotations

import logging
import random
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Optional, TypeVar

log = logging.getLogger("finops.ops")

T = TypeVar("T")


class ErrorCategory(str, Enum):
    TRANSIENT = "TRANSIENT"              # throttling/network/timeout -> bounded retry
    AUTH = "AUTH"                        # stop; alert owner/admin
    PERMISSION = "PERMISSION"            # stop; alert owner/admin
    SCHEMA_DRIFT = "SCHEMA_DRIFT"         # additive vs breaking decided by policy
    SOURCE_MISSING = "SOURCE_MISSING"    # optional -> SKIPPED_OPTIONAL; required -> fail
    DATA_QUALITY = "DATA_QUALITY"         # quarantine; enforce tolerance
    TARGET_CONFLICT = "TARGET_CONFLICT"   # retry transaction safely
    CHECKPOINT = "CHECKPOINT"            # stop source; controlled recovery
    FATAL = "FATAL"


_TRANSIENT_MARKERS = (
    "throttl", "rate limit", "too many requests", "timeout", "timed out",
    "temporarily unavailable", "connection reset", "connection refused",
    "503", "429", "deadlin", "please retry",
)
_AUTH_MARKERS = ("unauthorized", "unauthenticated", "invalid token", "expired token", "401")
_PERM_MARKERS = ("permission denied", "access denied", "insufficient privileges", "permission_denied", "403", "privil")
_MISSING_MARKERS = ("table or view not found", "does not exist", "nosuchtableexception")
_CONFLICT_MARKERS = ("concurrent", "conflict", "concurrentwriteexception", "transaction")


def classify_exception(exc: BaseException) -> tuple[ErrorCategory, bool]:
    """Classify an exception into (category, retryable-now)."""
    text = f"{type(exc).__name__}: {exc}".lower()
    if any(m in text for m in _MISSING_MARKERS) and "permission" not in text:
        return ErrorCategory.SOURCE_MISSING, False
    if any(m in text for m in _PERM_MARKERS):
        return ErrorCategory.PERMISSION, False
    if any(m in text for m in _AUTH_MARKERS):
        return ErrorCategory.AUTH, False
    if any(m in text for m in _CONFLICT_MARKERS):
        return ErrorCategory.TARGET_CONFLICT, True
    if any(m in text for m in _TRANSIENT_MARKERS):
        return ErrorCategory.TRANSIENT, True
    return ErrorCategory.FATAL, False


@dataclass(frozen=True)
class RetryPolicy:
    """Bounded exponential backoff with full jitter."""

    max_attempts: int = 3
    backoff_seconds: tuple = (30, 120, 300)
    jitter_fraction: float = 0.25
    sleep: Callable[[float], None] = time.sleep

    def delay(self, attempt: int, rand: Optional[random.Random] = None) -> float:
        """Delay before retry *attempt* (1-based): base * jitter."""
        rand = rand or random
        base = self.backoff_seconds[min(attempt - 1, len(self.backoff_seconds) - 1)]
        spread = base * self.jitter_fraction
        return max(0.0, base + rand.uniform(-spread, spread))

    def with_retry(self, fn: Callable[[], T], on_retry: Optional[Callable] = None) -> T:
        """Run *fn* with bounded retries; non-retryable errors raise immediately."""
        last: BaseException | None = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                return fn()
            except Exception as exc:  # noqa: BLE001 - classified below
                category, retryable = classify_exception(exc)
                if not retryable or attempt == self.max_attempts:
                    raise
                last = exc
                delay = self.delay(attempt)
                if on_retry:
                    on_retry(attempt, delay, category, exc)
                log.warning(
                    "retry %d/%d after %.1fs (%s): %s",
                    attempt, self.max_attempts, delay, category, str(exc)[:200],
                )
                self.sleep(delay)
        raise last  # pragma: no cover


@dataclass
class CircuitBreaker:
    """Per-source circuit breaker: OPEN after *threshold* consecutive failures.

    Failure counts are backed by ``ops.source_run`` so state survives restarts;
    the in-memory view caches the query result for the current run.
    """

    spark: object = None
    catalog: str = ""
    threshold: int = 5
    _state: dict[str, str] = field(default_factory=dict)

    def consecutive_failures(self, source_id: str, workspace_id: str, region: str) -> int:
        rows = self.spark.sql(
            f"""WITH ordered AS (
                  SELECT status,
                         row_number() OVER (ORDER BY started_at DESC) AS rn
                  FROM {self.catalog}.ops.source_run
                  WHERE source_id = '{source_id}'
                    AND workspace_id = '{workspace_id}' AND region = '{region}')
                SELECT count(*) AS failures FROM ordered
                WHERE rn <= {self.threshold} AND status = 'FAILED'"""
        ).collect()
        return int(rows[0].failures) if rows else 0

    def allow(self, source_id: str, workspace_id: str, region: str) -> tuple[bool, int]:
        failures = self.consecutive_failures(source_id, workspace_id, region)
        state = "OPEN" if failures >= self.threshold else "CLOSED"
        self._state[source_id] = state
        return state == "CLOSED", failures


class RunTracker:
    """Pipeline and source run lifecycle in ``ops.pipeline_run`` / ``ops.source_run``."""

    def __init__(self, spark, catalog: str):
        self.spark = spark
        self.catalog = catalog
        self.pipeline_table = f"{catalog}.ops.pipeline_run"
        self.source_table = f"{catalog}.ops.source_run"

    def new_run_id(self) -> str:
        return str(uuid.uuid4())

    def start_pipeline(self, run_id: str, environment: str, region: str,
                       workspace_id: str, config_version: str, effective_hash: str) -> None:
        self.spark.sql(
            f"""INSERT INTO {self.pipeline_table}
                VALUES ('{run_id}', '{environment}', '{region}', '{workspace_id}',
                        '{config_version}', '{effective_hash}', 'RUNNING',
                        current_timestamp(), null, null, null, null)"""
        )

    def finish_pipeline(self, run_id: str, status: str, sources_ok: int,
                        sources_failed: int, sources_skipped: int) -> None:
        self.spark.sql(
            f"""UPDATE {self.pipeline_table} SET finished_at = current_timestamp(),
                  status = '{status}', sources_ok = {sources_ok},
                  sources_failed = {sources_failed}, sources_skipped = {sources_skipped}
                WHERE run_id = '{run_id}' AND status = 'RUNNING'"""
        )

    def start_source(self, run_id: str, source_id: str, workspace_id: str, region: str,
                     lower_bound: str, upper_bound: str) -> None:
        self.spark.sql(
            f"""INSERT INTO {self.source_table}
                VALUES ('{run_id}', '{source_id}', '{workspace_id}', '{region}',
                        '{lower_bound}', '{upper_bound}', 'RUNNING',
                        current_timestamp(), null, null, null, null,
                        null, null, null, null)"""
        )

    def finish_source(self, run_id: str, source_id: str, status: str,
                      rows_read: int = 0, rows_inserted: int = 0, rows_updated: int = 0,
                      rows_quarantined: int = 0, retries: int = 0,
                      error_class: str = None, error_message: str = None) -> None:
        err = "null" if error_class is None else f"'{error_class}'"
        msg = "null" if error_message is None else "'" + error_message.replace("'", "''")[:2000] + "'"
        self.spark.sql(
            f"""UPDATE {self.source_table} SET finished_at = current_timestamp(), status = '{status}',
                  rows_read = {rows_read}, rows_inserted = {rows_inserted}, rows_updated = {rows_updated},
                  rows_quarantined = {rows_quarantined}, retries = {retries},
                  error_class = {err}, error_message = {msg}
                WHERE run_id = '{run_id}' AND source_id = '{source_id}' AND status = 'RUNNING'"""
        )


class AlertOutbox:
    """Reliable, deduplicated alert delivery queue in ``ops.alert_outbox``."""

    def __init__(self, spark, catalog: str):
        self.spark = spark
        self.table = f"{catalog}.ops.alert_outbox"

    def emit(self, alert_type: str, severity: str, message: str, dedupe_key: str,
             run_id: str, source_id: str = "") -> None:
        """Append an alert unless the same dedupe_key is already UNSENT."""
        existing = self.spark.sql(
            f"""SELECT count(*) AS n FROM {self.table}
                WHERE dedupe_key = '{dedupe_key}' AND delivery_status = 'UNSENT'"""
        ).collect()
        if existing and existing[0].n > 0:
            log.info("alert deduplicated: %s", dedupe_key)
            return
        self.spark.sql(
            f"""INSERT INTO {self.table}
                VALUES ('{uuid.uuid4()}', '{alert_type}', '{severity}',
                        '{message.replace(chr(39), chr(39)*2)[:1000]}',
                        '{dedupe_key}', '{run_id}', '{source_id}',
                        current_timestamp(), null, 'UNSENT')"""
        )


class FreshnessMonitor:
    """Refreshes ``ops.freshness_status`` from the latest landed event times."""

    def __init__(self, spark, catalog: str):
        self.spark = spark
        self.catalog = catalog

    def evaluate(self, source_id: str, silver_table: str | None, event_time_column: str,
                 expected_minutes: int, warning_minutes: int, critical_minutes: int) -> str:
        """UpSERT one source freshness row; returns the computed status."""
        table = f"{self.catalog}.silver.{silver_table}" if silver_table else None
        if table:
            rows = self.spark.sql(
                f"""SELECT max({event_time_column}) AS latest, count(*) AS n FROM {table}"""
            ).collect()
            latest_event = rows[0].latest if rows else None
            row_count = rows[0].n if rows else 0
        else:
            rows = self.spark.sql(
                f"""SELECT max(source_event_time) AS latest, count(*) AS n
                    FROM {self.catalog}.bronze.{source_id}__raw"""
            ).collect()
            latest_event = rows[0].latest if rows else None
            row_count = rows[0].n if rows else 0

        if latest_event is None:
            status, lag_minutes = "NO_DATA", None
        else:
            lag_rows = self.spark.sql(
                f"""SELECT greatest(0, cast((unix_timestamp(current_timestamp())
                       - unix_timestamp(timestamp '{latest_event}')) / 60 AS INT)) AS lag"""
            ).collect()
            lag_minutes = int(lag_rows[0].lag) if lag_rows else None
            if lag_minutes is None:
                status = "UNKNOWN"
            elif lag_minutes <= expected_minutes + warning_minutes:
                status = "FRESH" if lag_minutes <= expected_minutes else "WARNING"
            elif lag_minutes <= critical_minutes:
                status = "WARNING"
            else:
                status = "CRITICAL"

        lag_sql = "null" if lag_minutes is None else str(lag_minutes)
        latest_sql = "null" if latest_event is None else f"timestamp '{latest_event}'"
        self.spark.sql(
            f"""DELETE FROM {self.catalog}.ops.freshness_status WHERE source_id = '{source_id}'"""
        )
        self.spark.sql(
            f"""INSERT INTO {self.catalog}.ops.freshness_status
                VALUES ('{source_id}', {latest_sql}, {lag_sql}, '{status}',
                        {expected_minutes}, {warning_minutes}, {critical_minutes},
                        {row_count}, current_timestamp())"""
        )
        return status
