"""Parameterized SQL file runner.

Reusable SQL transformations live in ``src/sql/`` as files with ``{placeholder}``
substitution tokens. This module renders them with named parameters and splits
them into executable statements. Notebooks and the pipeline stay thin.
"""
from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Iterable

log = logging.getLogger("finops.sql")

_PLACEHOLDER = re.compile(r"\{([a-z_][a-z0-9_]*)\}")


class SqlTemplate:
    """A SQL file rendered with named parameters."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self._text = self.path.read_text(encoding="utf-8")

    def render(self, **params: Any) -> str:
        """Substitute ``{name}`` tokens; unknown tokens are an error."""
        missing: list[str] = []

        def _sub(match: re.Match[str]) -> str:
            key = match.group(1)
            if key not in params:
                missing.append(key)
                return match.group(0)
            return str(params[key])

        rendered = _PLACEHOLDER.sub(_sub, self._text)
        if missing:
            raise ValueError(
                f"SQL template {self.path} has unresolved placeholders: {sorted(set(missing))}"
            )
        return rendered

    def statements(self, **params: Any) -> list[str]:
        """Rendered SQL split into individual statements (``;`` line terminators)."""
        text = self.render(**params)
        stmts: list[str] = []
        buf: list[str] = []
        for line in text.splitlines():
            buf.append(line)
            stripped = line.strip()
            if stripped.endswith(";") and not stripped.startswith("--"):
                stmt = "\n".join(buf).strip().rstrip(";").strip()
                if stmt:
                    stmts.append(stmt)
                buf = []
        tail = "\n".join(buf).strip()
        if tail:
            stmts.append(tail)
        return stmts


def run_sql_file(spark: Any, path: str | Path, **params: Any) -> list[Any]:
    """Execute every statement of a SQL file; return the last result per statement."""
    tpl = SqlTemplate(path)
    results = []
    for stmt in tpl.statements(**params):
        log.debug("executing statement from %s: %s...", tpl.path, stmt[:120])
        results.append(spark.sql(stmt))
    return results
