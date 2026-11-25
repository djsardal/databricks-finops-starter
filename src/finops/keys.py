"""Deterministic record identity and payload hashing.

Bronze ``record_id`` derivation (documented, mirrored exactly by the SQL the
Bronze writer renders — ``sha2(concat_ws('|', cloud, account_id, workspace_id,
region, source_system, source_table, source_record_id), 256)``). Source IDs are
never invented: ``source_record_id`` is a real source key column or a documented
concatenation of stable source key columns declared in the source YAML
(``derived_keys.source_record_id_expression``).

The same record ingested twice produces the same ``record_id`` and the same
``payload_hash``; merges therefore make replay idempotent.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping, Optional


def sha256_hex(text: str) -> str:
    """Return the hex SHA-256 digest of *text* (lowercase, 64 chars)."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def build_source_identity(
    cloud: str,
    account_id: str,
    workspace_id: str,
    region: str,
    source_system: str,
    source_table: str,
    source_record_id: Optional[str],
) -> str:
    """Canonical pipe-joined source identity string (matches SQL concat_ws)."""
    return "|".join(
        [
            str(cloud or ""),
            str(account_id or ""),
            str(workspace_id or ""),
            str(region or ""),
            str(source_system or ""),
            str(source_table or ""),
            str(source_record_id or ""),
        ]
    )


def deterministic_record_id(
    cloud: str,
    account_id: str,
    workspace_id: str,
    region: str,
    source_system: str,
    source_table: str,
    source_record_id: Optional[str],
) -> str:
    """Deterministic Bronze record identity.

    Mirrors the SQL ``sha2(concat_ws('|', ...), 256)`` used by the Bronze
    writer, so records computed in Python and in SQL always agree.
    """
    return sha256_hex(
        build_source_identity(
            cloud, account_id, workspace_id, region, source_system, source_table, source_record_id
        )
    )


def canonical_json(value: Any) -> str:
    """Canonical JSON: sorted keys, compact separators, deterministic."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str, ensure_ascii=False)


def payload_hash(payload: Mapping[str, Any] | Any) -> str:
    """Stable payload hash over the canonical JSON representation."""
    return sha256_hex(canonical_json(payload))
