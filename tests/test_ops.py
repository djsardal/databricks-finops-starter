"""Unit tests: error classification, bounded retry with backoff/jitter."""
import random

import pytest

from finops.ops import ErrorCategory, RetryPolicy, classify_exception


def test_classification_transient_retryable():
    category, retryable = classify_exception(RuntimeError("Request timed out after 30s"))
    assert category is ErrorCategory.TRANSIENT and retryable


def test_classification_permission_not_retryable():
    category, retryable = classify_exception(RuntimeError("PERMISSION_DENIED on system.billing.usage"))
    assert category is ErrorCategory.PERMISSION and not retryable


def test_classification_missing_table():
    category, retryable = classify_exception(RuntimeError("Table or view not found: x"))
    assert category is ErrorCategory.SOURCE_MISSING and not retryable


def test_classification_unknown_is_fatal():
    category, retryable = classify_exception(ValueError("something odd"))
    assert category is ErrorCategory.FATAL and not retryable


def test_retry_delay_within_jitter_bounds():
    policy = RetryPolicy(backoff_seconds=(30, 120))
    rand = random.Random(42)
    d1 = policy.delay(1, rand)
    assert 30 * 0.75 <= d1 <= 30 * 1.25
    for attempt in (2, 5, 9):  # attempts beyond the tuple clamp to the last bucket
        d = policy.delay(attempt, rand)
        assert 120 * 0.75 <= d <= 120 * 1.25


def test_with_retry_recovers_from_transient_failures():
    policy = RetryPolicy(max_attempts=3, backoff_seconds=(0, 0, 0))
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("503 temporarily unavailable")
        return "ok"

    assert policy.with_retry(flaky) == "ok"
    assert calls["n"] == 3


def test_with_retry_does_not_retry_permission_errors():
    policy = RetryPolicy(max_attempts=3, backoff_seconds=(0, 0, 0))
    calls = {"n": 0}

    def denied():
        calls["n"] += 1
        raise RuntimeError("403 access denied")

    with pytest.raises(RuntimeError):
        policy.with_retry(denied)
    assert calls["n"] == 1  # raised immediately, no retries
