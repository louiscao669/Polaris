"""Standard event envelope for async writes (Kafka)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class EventMetadata(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    timestamp: str = Field(default_factory=utc_now_iso)
    schema_version: int = 1
    source: str = "polaris-gateway"
    domain: str
    sub: str | None = None
    org_id: int | str | None = None
    scopes: list[str] | None = None


class EventEnvelope(BaseModel):
    metadata: EventMetadata
    payload: dict[str, Any]


def _scopes_from_claims(claims: dict[str, Any] | None) -> list[str] | None:
    if not claims:
        return None
    raw = claims.get("scopes")
    if isinstance(raw, list):
        return [str(x) for x in raw]
    sc = claims.get("scope")
    if isinstance(sc, str):
        return sc.split()
    return None


def _org_from_claims(claims: dict[str, Any] | None) -> int | str | None:
    if not claims:
        return None
    v = claims.get("org_id")
    if v is None:
        return None
    if isinstance(v, int):
        return v
    s = str(v)
    return int(s) if s.isdigit() else s


def build_envelope(
    *,
    domain: str,
    payload: dict[str, Any],
    jwt_claims: dict[str, Any] | None = None,
) -> EventEnvelope:
    md = EventMetadata(domain=domain)
    if jwt_claims:
        md.sub = jwt_claims.get("sub")
        md.org_id = _org_from_claims(jwt_claims)
        md.scopes = _scopes_from_claims(jwt_claims)
    return EventEnvelope(metadata=md, payload=payload)


def merge_actor_into_payload(
    payload: dict[str, Any], jwt_claims: dict[str, Any] | None
) -> dict[str, Any]:
    """Prefer explicit user_id on payload; then JWT user_id claim; then numeric sub."""
    out = dict(payload)
    if jwt_claims is None:
        return out
    if "user_id" in out:
        return out
    uid = jwt_claims.get("user_id")
    if uid is not None:
        out["user_id"] = uid
        return out
    sub = jwt_claims.get("sub")
    if sub is not None and str(sub).isdigit():
        out["user_id"] = int(sub)
    return out
