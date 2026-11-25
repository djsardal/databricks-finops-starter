"""Unit/integration tests: SQL template rendering and statement splitting."""
import pytest

from finops.sqlrunner import SqlTemplate

PARAMS = {"catalog": "finops_dev_test", "silver_lower": "1970-01-01 00:00:00"}


def test_render_and_split_silver_file(repo_root):
    tpl = SqlTemplate(repo_root / "src/sql/silver/billing_list_prices.sql")
    text = tpl.render(**PARAMS)
    assert "{catalog}" not in text
    assert "finops_dev_test.bronze.billing_list_prices_raw" in text
    stmts = tpl.statements(**PARAMS)
    assert len(stmts) == 2  # CREATE TABLE + MERGE
    assert all(s.strip() for s in stmts)


def test_unresolved_placeholder_raises(repo_root):
    tpl = SqlTemplate(repo_root / "src/sql/silver/billing_usage.sql")
    with pytest.raises(ValueError, match="unresolved placeholders"):
        tpl.render(catalog="c")  # silver_lower missing


def test_bootstrap_renders_full_control_plane(repo_root):
    tpl = SqlTemplate(repo_root / "src/sql/ddl/bootstrap_regional.sql")
    stmts = tpl.statements(catalog="finops_dev_test")
    # 1 catalog + 6 schemas + 11 ops/config tables
    assert len(stmts) >= 16
    assert any("CREATE CATALOG" in s for s in stmts)
    assert any("source_checkpoint" in s for s in stmts)
