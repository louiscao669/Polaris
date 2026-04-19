"""HTTP entrypoints for async (Kafka-backed) writes."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status

from backend.app.auth.jwt_deps import (
    bearer_claims_required_for_operations_read,
    bearer_claims_required_for_writes,
)
from backend.app.operations_repo import (
    fetch_operation,
    insert_operation_pending,
    update_operation_kafka_meta,
    use_writer_for_operation_fetch,
)
from backend.app.schemas.envelope import (
    build_envelope,
    merge_actor_into_payload,
)
from backend.app.settings_jwt import (
    V2_OPERATIONS_REQUIRE_JWT,
    V2_REQUIRE_JWT,
)
from backend.app.topics import MARKET_OPERATIONS, ORG_MANAGEMENT, USER_ACCOUNT
from backend.app.v2_kafka_client import v2_kafka_producer


router = APIRouter(prefix="/v2", tags=["v2-async"])


def _estimate_completion() -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=5)).isoformat()


def _merge_payload(
    payload: dict[str, Any],
    claims: dict[str, Any],
    x_user_id: str | None,
) -> dict[str, Any]:
    body = merge_actor_into_payload(payload, claims if claims else None)
    if not V2_REQUIRE_JWT and x_user_id and "user_id" not in body:
        body = {**body, "user_id": x_user_id}
    return body


@router.post("/markets/transactions")
async def v2_market_transaction(
    payload: dict[str, Any],
    claims: Annotated[dict[str, Any], Depends(bearer_claims_required_for_writes)],
    x_user_id: str | None = Header(default=None, alias="X-User-Id"),
) -> Response:
    body = _merge_payload(payload, claims, x_user_id)
    env = build_envelope(
        domain="market.operations",
        payload=body,
        jwt_claims=claims if claims else None,
    )
    oid = env.metadata.event_id
    envelope_dict = env.model_dump(mode="json")

    market_id = body.get("market_id") or body.get("marketId")
    key = str(market_id).encode("utf-8") if market_id is not None else None

    part, off = await v2_kafka_producer.send_json(
        topic=MARKET_OPERATIONS, value=envelope_dict, key=key
    )
    insert_operation_pending(
        operation_id=oid, topic=MARKET_OPERATIONS, envelope=envelope_dict
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


@router.post("/org/management")
async def v2_org_management(
    payload: dict[str, Any],
    claims: Annotated[dict[str, Any], Depends(bearer_claims_required_for_writes)],
    x_user_id: str | None = Header(default=None, alias="X-User-Id"),
) -> Response:
    body = _merge_payload(payload, claims, x_user_id)
    env = build_envelope(
        domain="org.management",
        payload=body,
        jwt_claims=claims if claims else None,
    )
    oid = env.metadata.event_id
    envelope_dict = env.model_dump(mode="json")

    org_id = body.get("organization_id") or body.get("org_id")
    key = str(org_id).encode("utf-8") if org_id is not None else None

    part, off = await v2_kafka_producer.send_json(
        topic=ORG_MANAGEMENT, value=envelope_dict, key=key
    )
    insert_operation_pending(
        operation_id=oid, topic=ORG_MANAGEMENT, envelope=envelope_dict
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


@router.post("/user/account")
async def v2_user_account(
    payload: dict[str, Any],
    claims: Annotated[dict[str, Any], Depends(bearer_claims_required_for_writes)],
    x_user_id: str | None = Header(default=None, alias="X-User-Id"),
) -> Response:
    body = _merge_payload(payload, claims, x_user_id)
    env = build_envelope(
        domain="user.account",
        payload=body,
        jwt_claims=claims if claims else None,
    )
    oid = env.metadata.event_id
    envelope_dict = env.model_dump(mode="json")

    uid = body.get("user_id")
    key = str(uid).encode("utf-8") if uid is not None else None

    part, off = await v2_kafka_producer.send_json(
        topic=USER_ACCOUNT, value=envelope_dict, key=key
    )
    insert_operation_pending(
        operation_id=oid, topic=USER_ACCOUNT, envelope=envelope_dict
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


@router.get("/operations/{operation_id}")
async def v2_get_operation(
    operation_id: UUID,
    claims: Annotated[dict[str, Any], Depends(bearer_claims_required_for_operations_read)],
    x_force_leader: str | None = Header(default=None, alias="X-Force-Leader"),
) -> dict[str, Any]:
    row = fetch_operation(
        operation_id,
        use_writer=use_writer_for_operation_fetch(x_force_leader),
    )
    if row is None:
        raise HTTPException(status_code=404, detail="operation not found")

    if V2_OPERATIONS_REQUIRE_JWT:
        env = row.get("envelope")
        meta = env.get("metadata") if isinstance(env, dict) else {}
        owner_sub = meta.get("sub") if isinstance(meta, dict) else None
        viewer_sub = claims.get("sub") if claims else None
        if owner_sub is None or viewer_sub is None or owner_sub != viewer_sub:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="not allowed to read this operation",
            )

    return row
