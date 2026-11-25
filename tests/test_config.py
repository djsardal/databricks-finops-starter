"""Configuration tests against the REAL conf/ tree (YAML control plane).

These run in any Python environment with the repo checked out; they need no
Databricks session (loader + validator are pure functions over YAML).
"""
import pytest

from finops.config import SECRET_ERROR_PATTERNS, EffectiveConfig


def test_real_conf_loads_and_validates(repo_root):
    cfg = EffectiveConfig.load(repo_root / "conf", "dev")
    assert cfg.environment == "dev"
    for sid in ("billing_usage", "billing_list_prices", "query_history", "genie_usage"):
        assert sid in cfg.sources, f"source {sid} missing from conf"
    assert cfg.sources["genie_usage"].is_derived
    assert cfg.sources["genie_usage"].derived_from == "billing_usage"
    for sid in ("billing_usage", "billing_list_prices"):
        assert cfg.sources[sid].required, f"{sid} must be required for the cost model"


def test_effective_hash_is_stable(repo_root):
    a = EffectiveConfig.load(repo_root / "conf", "dev")
    b = EffectiveConfig.load(repo_root / "conf", "dev")
    assert a.effective_hash == b.effective_hash
    assert len(a.effective_hash) == 64


def test_unknown_environment_rejected(repo_root):
    with pytest.raises(Exception):
        EffectiveConfig.load(repo_root / "conf", "staging")


def test_secret_leak_patterns():
    assert SECRET_ERROR_PATTERNS["databricks_token"].search("token dapi-0123456789abcdef0123456789abcdef")
    assert SECRET_ERROR_PATTERNS["assigned_credential_value"].search("password: hunter2secret")
    # references/placeholders must NOT be flagged as leaked values
    assert not SECRET_ERROR_PATTERNS["assigned_credential_value"].search("client_secret: {secret_scope}")
    assert not SECRET_ERROR_PATTERNS["databricks_token"].search("no tokens here")
