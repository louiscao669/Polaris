"""Unit tests for envelope construction and actor merging (no Kafka/DB)."""

from __future__ import annotations

from uuid import UUID

import pytest

from app.schemas.envelope import build_envelope, merge_actor_into_payload


def test_merge_actor_prefers_explicit_user_id_over_jwt() -> None:
    claims = {"sub": "99", "user_id": 7}
    out = merge_actor_into_payload({"user_id": 42, "action": "MARKET_TRANSACTION"}, claims)
    assert out["user_id"] == 42


def test_merge_actor_takes_user_id_from_claims() -> None:
    claims = {"user_id": 7}
    out = merge_actor_into_payload({"action": "MARKET_TRANSACTION"}, claims)
    assert out["user_id"] == 7


def test_merge_actor_maps_numeric_sub_to_user_id() -> None:
    claims = {"sub": "123"}
    out = merge_actor_into_payload({"action": "MARKET_TRANSACTION"}, claims)
    assert out["user_id"] == 123


def test_merge_actor_non_numeric_sub_does_not_set_user_id() -> None:
    claims = {"sub": "alice"}
    out = merge_actor_into_payload({"action": "MARKET_TRANSACTION"}, claims)
    assert "user_id" not in out


def test_build_envelope_includes_claims_metadata() -> None:
    claims = {"sub": "test-sub", "org_id": 3, "scope": "markets:write events:read"}
    env = build_envelope(domain="market.operations", payload={"x": 1}, jwt_claims=claims)
    assert env.metadata.domain == "market.operations"
    assert env.metadata.sub == "test-sub"
    assert env.metadata.org_id == 3
    assert env.metadata.scopes == ["markets:write", "events:read"]
    assert env.payload == {"x": 1}


def test_build_envelope_generates_stable_uuid_field() -> None:
    env = build_envelope(domain="market.operations", payload={}, jwt_claims=None)
    assert isinstance(env.metadata.event_id, UUID)
