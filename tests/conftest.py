"""Shared fixtures: repo root on sys.path so pure-Python tests can import finops."""
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

# Module-level (not fixture) so it applies during COLLECTION, before test
# modules execute their top-level `import finops...` statements.
_SRC = str(REPO_ROOT / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return REPO_ROOT
