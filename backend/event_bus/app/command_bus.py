"""Shared helpers for Kafka-backed command submission."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import Response

from .operations_repo import (
    insert_operation_pending,
    update_operation_kafka_meta,
)
from .schemas.envelope import (
    build_envelope,
    merge_actor_into_payload,
)
from .settings_jwt import V2_REQUIRE_JWT
from .topics import (
    EVENT_LIFECYCLE,
    MARKET_OPERATIONS,
    ORG_MANAGEMENT,
    USER_ACCOUNT,
)
from .v2_kafka_client import v2_kafka_producer


def _estimate_completion() -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=5)).isoformat()


def merge_command_payload(
    payload: dict[str, Any],
    *,
    claims: dict[str, Any] | None = None,
    x_user_id: str | None = None,
) -> dict[str, Any]:
    body = merge_actor_into_payload(payload, claims if claims else None)
    if not V2_REQUIRE_JWT and x_user_id and "user_id" not in body:
        body = {**body, "user_id": x_user_id}
    return body


def _key_for_topic(topic: str, payload: dict[str, Any]) -> bytes | None:
    if topic == MARKET_OPERATIONS:
        source = (
            payload.get("market_id")
            or payload.get("marketId")
            or payload.get("event_id")
            or payload.get("eventId")
        )
    elif topic == EVENT_LIFECYCLE:
        # Market v2 payloads are published on this topic too; use the same key
        # ordering as legacy market.operations (event_id when present).
        source = (
            payload.get("event_id")
            or payload.get("eventId")
            or payload.get("market_id")
            or payload.get("marketId")
        )
    elif topic == ORG_MANAGEMENT:
        source = payload.get("organization_id") or payload.get("org_id")
    elif topic == USER_ACCOUNT:
        source = payload.get("user_id")
    else:
        source = None
    return str(source).encode("utf-8") if source is not None else None


async def enqueue_command_response(
    *,
    topic: str,
    domain: str,
    payload: dict[str, Any],
    claims: dict[str, Any] | None = None,
    x_user_id: str | None = None,
) -> Response:
    body = merge_command_payload(payload, claims=claims, x_user_id=x_user_id)
    env = build_envelope(
        domain=domain,
        payload=body,
        jwt_claims=claims if claims else None,
    )
    oid = env.metadata.event_id
    envelope_dict = env.model_dump(mode="json")

    part, off = await v2_kafka_producer.send_json(
        topic=topic,
        value=envelope_dict,
        key=_key_for_topic(topic, body),
    )
    insert_operation_pending(
        operation_id=oid,
        topic=topic,
        envelope=envelope_dict,
    )
    update_operation_kafka_meta(operation_id=oid, partition=part, offset=off)

    out = {
        "accepted": True,
        "operation_id": str(oid),
        "status": "queued",
        "received_at": env.metadata.timestamp,
        "estimated_completion": _estimate_completion(),
    }
    return Response(
        content=json.dumps(out),
        media_type="application/json",
        status_code=202,
    )
