"""HTTP entrypoints for async (Kafka-backed) writes."""

from __future__ import annotations

from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status

from .command_bus import enqueue_command_response
from .auth.jwt_deps import (
    bearer_claims_optional,
    bearer_claims_required_for_operations_read,
    bearer_claims_required_for_writes,
)

from .operations_repo import fetch_operation, use_writer_for_operation_fetch
from .settings_jwt import (
    V2_OPERATIONS_REQUIRE_JWT,
)
from .topics import EVENT_LIFECYCLE, MARKET_OPERATIONS, ORG_MANAGEMENT, USER_ACCOUNT


router = APIRouter(prefix="/v2", tags=["v2-async"])


PUBLIC_USER_ACCOUNT_ACTIONS = frozenset({"USER_SIGNUP", "USER_LOGIN"})


def _claims_for_user_account_action(
    payload: dict[str, Any],
    claims: dict[str, Any] | None,
) -> dict[str, Any]:
    action = str(payload.get("action") or "").strip().upper()
    if action in PUBLIC_USER_ACCOUNT_ACTIONS:
        return claims or {}
    if claims:
        return claims
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="missing bearer token",
    )


@router.post("/markets/transactions")
async def v2_market_transaction(
    payload: dict[str, Any],
    claims: Annotated[dict[str, Any], Depends(bearer_claims_required_for_writes)],
    x_user_id: str | None = Header(default=None, alias="X-User-Id"),
) -> Response:
    return await enqueue_command_response(
        topic=MARKET_OPERATIONS,
        domain="market.operations",
        payload=payload,
        claims=claims,
        x_user_id=x_user_id,
    )


@router.post("/markets/payout")
async def v2_market_payout(
    payload: dict[str, Any],
    claims: Annotated[dict[str, Any], Depends(bearer_claims_required_for_writes)],
    x_user_id: str | None = Header(default=None, alias="X-User-Id"),
) -> Response:
    return await enqueue_command_response(
        topic=MARKET_OPERATIONS,
        domain="market.operations",
        payload=payload,
        claims=claims,
        x_user_id=x_user_id,
    )


@router.post("/markets/lifecycle")
async def v2_market_lifecycle(
    payload: dict[str, Any],
    claims: Annotated[dict[str, Any], Depends(bearer_claims_required_for_writes)],
    x_user_id: str | None = Header(default=None, alias="X-User-Id"),
) -> Response:
    return await enqueue_command_response(
        topic=MARKET_OPERATIONS,
        domain="market.operations",
        payload=payload,
        claims=claims,
        x_user_id=x_user_id,
    )


@router.post("/org/management")
async def v2_org_management(
    payload: dict[str, Any],
    claims: Annotated[dict[str, Any], Depends(bearer_claims_required_for_writes)],
    x_user_id: str | None = Header(default=None, alias="X-User-Id"),
) -> Response:
    return await enqueue_command_response(
        topic=ORG_MANAGEMENT,
        domain="org.management",
        payload=payload,
        claims=claims,
        x_user_id=x_user_id,
    )


@router.post("/events/lifecycle")
async def v2_event_lifecycle(
    payload: dict[str, Any],
    claims: Annotated[dict[str, Any], Depends(bearer_claims_required_for_writes)],
    x_user_id: str | None = Header(default=None, alias="X-User-Id"),
) -> Response:
    return await enqueue_command_response(
        topic=EVENT_LIFECYCLE,
        domain="event.lifecycle",
        payload=payload,
        claims=claims,
        x_user_id=x_user_id,
    )


@router.post("/user/account")
async def v2_user_account(
    payload: dict[str, Any],
    claims: Annotated[dict[str, Any] | None, Depends(bearer_claims_optional)],
    x_user_id: str | None = Header(default=None, alias="X-User-Id"),
) -> Response:
    effective_claims = _claims_for_user_account_action(payload, claims)
    return await enqueue_command_response(
        topic=USER_ACCOUNT,
        domain="user.account",
        payload=payload,
        claims=effective_claims,
        x_user_id=x_user_id,
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
