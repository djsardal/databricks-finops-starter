"""Unit tests: deterministic record identity and payload hashing."""
from finops.keys import (
    canonical_json,
    deterministic_record_id,
    payload_hash,
    sha256_hex,
)


def test_sha256_known_vector():
    assert (
        sha256_hex("abc")
        == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    )


def test_record_id_deterministic_and_scope_sensitive():
    args = ("aws", "acct-1", "ws-1", "us-east-1", "system_table", "system.billing.usage")
    a = deterministic_record_id(*args, "rec-1")
    b = deterministic_record_id(*args, "rec-1")
    c = deterministic_record_id(*args, "rec-2")
    d = deterministic_record_id("aws", "acct-1", "ws-2", "us-east-1",
                                "system_table", "system.billing.usage", "rec-1")
    assert a == b          # same input -> same id (idempotent merges)
    assert a != c          # different source record -> different id
    assert a != d          # different workspace -> different id (collision-safe)
    assert len(a) == 64    # sha-256 hex


def test_canonical_json_is_key_sorted_and_compact():
    assert canonical_json({"b": 1, "a": 2}) == '{"a":2,"b":1}'


def test_payload_hash_stable_and_content_sensitive():
    payload = {"x": [1, {"y": 2}], "z": "text"}
    assert payload_hash(payload) == payload_hash({"z": "text", "x": [1, {"y": 2}]})
    assert payload_hash({"x": 1}) != payload_hash({"x": 2})
