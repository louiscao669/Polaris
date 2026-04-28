"""Direct HTTP entrypoints for backend functions under ``core/Backend Functions``."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Body, HTTPException, Query
from pymysql.cursors import DictCursor

from .command_bus import enqueue_command_response
from .read_cache import cache_mode
from .database import get_connection_reader, get_connection_writer
from .topics import EVENT_LIFECYCLE, MARKET_OPERATIONS, ORG_MANAGEMENT, USER_ACCOUNT

_BF_ROOT = Path(__file__).resolve().parent / "core" / "Backend Functions"
for _dir in (
    _BF_ROOT,
    _BF_ROOT / "Organization Functions",
    _BF_ROOT / "Event Functions",
    _BF_ROOT / "Market Functions",
    _BF_ROOT / "User Functions",
):
    _path = str(_dir)
    if _path not in sys.path:
        sys.path.insert(0, _path)


def _load_module(unique_name: str, file_path: Path):
    spec = importlib.util.spec_from_file_location(unique_name, file_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[unique_name] = module
    spec.loader.exec_module(module)
    return module


_read_org = _load_module(
    "polaris_bf_read_organization",
    _BF_ROOT / "Organization Functions" / "Read_Organization.py",
)
_write_org = _load_module(
    "polaris_bf_write_organization_http",
    _BF_ROOT / "Organization Functions" / "Write_Organization.py",
)
_update_org = _load_module(
    "polaris_bf_update_organization_http",
    _BF_ROOT / "Organization Functions" / "Update_Organization.py",
)
_read_event = _load_module(
    "polaris_bf_read_event_http",
    _BF_ROOT / "Event Functions" / "Read_Event.py",
)
_write_event = _load_module(
    "polaris_bf_write_event_http",
    _BF_ROOT / "Event Functions" / "Write_Event.py",
)
_update_event = _load_module(
    "polaris_bf_update_event_http",
    _BF_ROOT / "Event Functions" / "Update_Event.py",
)
_read_market = _load_module(
    "polaris_bf_read_market_http",
    _BF_ROOT / "Market Functions" / "Read_Market.py",
)
_write_market = _load_module(
    "polaris_bf_write_market_http",
    _BF_ROOT / "Market Functions" / "Write_Market.py",
)
_update_market = _load_module(
    "polaris_bf_update_market_http",
    _BF_ROOT / "Market Functions" / "Update_Market.py",
)
_write_user = _load_module(
    "polaris_bf_write_user_http",
    _BF_ROOT / "User Functions" / "Write_User.py",
)
_update_user = _load_module(
    "polaris_bf_update_user_http",
    _BF_ROOT / "User Functions" / "Update_User.py",
)

router = APIRouter(tags=["backend-functions"])

DEFAULT_CONSTRAINT_TYPES = (
    (
        1,
        "max_bets_per_user",
        "Maximum number of bets a single user can place in this market or event.",
    ),
    (
        2,
        "max_user_volume",
        "Maximum total token volume a single user can trade in this market or event.",
    ),
    (
        3,
        "max_total_volume",
        "Maximum combined token volume allowed across the entire market or event.",
    ),
    (
        4,
        "max_yes_exposure_per_user",
        "Maximum number of YES-side tickets a single user can hold.",
    ),
    (
        5,
        "max_no_exposure_per_user",
        "Maximum number of NO-side tickets a single user can hold.",
    ),
)


def _unwrap_result(result: Any) -> Any:
    if not isinstance(result, dict) or result.get("ok") is not False:
        return result

    err = result.get("error", "validation")
    detail = result.get("message", "Request failed")
    status_code = {
        "auth": 401,
        "permission": 403,
        "validation": 422,
        "duplicate": 409,
        "precondition": 400,
        "not_open": 409,
        "not_closed": 409,
    }.get(err, 400)
    raise HTTPException(status_code=status_code, detail=detail)


def _apply(fn, payload: dict[str, Any]) -> Any:
    return _unwrap_result(fn(payload))


def _apply_with_cache_mode(
    fn,
    payload: dict[str, Any],
    *,
    request_cache_mode: str = "default",
) -> Any:
    with cache_mode(request_cache_mode):
        return _apply(fn, payload)


def _read_user_portfolio(user_id: int) -> dict[str, Any]:
    with get_connection_reader() as conn:
        cur = conn.cursor(DictCursor)

        cur.execute(
            """
            SELECT
                uts.token_id,
                uts.qty,
                ot.name AS token_name,
                ot.description AS token_description,
                ot.org_id AS organization_id,
                o.name AS organization_name
            FROM user_token_stock uts
            INNER JOIN organization_token ot ON ot.token_id = uts.token_id
            INNER JOIN organization o ON o.org_id = ot.org_id
            WHERE uts.user_id = %s AND uts.qty > 0
            ORDER BY o.name ASC, ot.name ASC, uts.token_id ASC
            """,
            (user_id,),
        )
        token_balances = [
            {
                "token_id": int(row["token_id"]),
                "qty": int(row["qty"]),
                "token_name": row["token_name"],
                "token_description": row["token_description"] or "",
                "organization_id": int(row["organization_id"]),
                "organization_name": row["organization_name"],
            }
            for row in cur.fetchall()
        ]

        cur.execute(
            """
            SELECT
                umt.market_id,
                umt.side,
                umt.qty,
                m.question,
                m.is_open,
                e.event_id,
                e.caption AS event_caption,
                e.org_id AS organization_id,
                o.name AS organization_name
            FROM user_market_ticket umt
            INNER JOIN market m ON m.id = umt.market_id
            INNER JOIN events e ON e.event_id = m.event_id
            INNER JOIN organization o ON o.org_id = e.org_id
            WHERE umt.user_id = %s AND umt.qty > 0
            ORDER BY o.name ASC, e.caption ASC, m.question ASC, umt.side DESC
            """,
            (user_id,),
        )
        open_tickets = [
            {
                "market_id": int(row["market_id"]),
                "side": bool(row["side"]),
                "qty": int(row["qty"]),
                "question": row["question"],
                "is_open": bool(row["is_open"]),
                "event_id": int(row["event_id"]),
                "event_caption": row["event_caption"],
                "organization_id": int(row["organization_id"]),
                "organization_name": row["organization_name"],
            }
            for row in cur.fetchall()
        ]

        return {
            "user_id": int(user_id),
            "token_balances": token_balances,
            "open_tickets": open_tickets,
        }


def _read_policy_metadata() -> dict[str, Any]:
    with get_connection_writer() as conn:
        cur = conn.cursor()
        cur.executemany(
            """
            INSERT IGNORE INTO constraint_type (constraint_id, name, description)
            VALUES (%s, %s, %s)
            """,
            DEFAULT_CONSTRAINT_TYPES,
        )
        cur.execute(
            """
            INSERT IGNORE INTO market_as (as_code, description) VALUES
            ('better', 'can place bets in the market'),
            ('viewer', 'can view market data'),
            ('analytic', 'can access market analytics')
            """
        )
        conn.commit()

    with get_connection_reader() as conn:
        cur = conn.cursor(DictCursor)

        cur.execute(
            """
            SELECT constraint_id, name, description
            FROM constraint_type
            ORDER BY name ASC, constraint_id ASC
            """
        )
        constraints = [
            {
                "constraint_id": int(row["constraint_id"]),
                "name": row["name"],
                "description": row["description"] or "",
            }
            for row in cur.fetchall()
        ]

        cur.execute(
            """
            SELECT as_code, description
            FROM market_as
            ORDER BY as_code ASC
            """
        )
        market_access = [
            {
                "as_code": row["as_code"],
                "description": row["description"] or "",
            }
            for row in cur.fetchall()
        ]

        return {
            "constraints": constraints,
            "market_access": market_access,
        }


def _read_org_join_options(organization_id: int) -> dict[str, Any]:
    with get_connection_reader() as conn:
        cur = conn.cursor(DictCursor)
        cur.execute(
            """
            SELECT org_id, name, description
            FROM organization
            WHERE org_id = %s
            """,
            (organization_id,),
        )
        organization = cur.fetchone()
        if organization is None:
            raise HTTPException(status_code=404, detail="That organization does not exist.")

        cur.execute(
            """
            SELECT role, description
            FROM organization_role
            WHERE org_id = %s
            ORDER BY role ASC
            """,
            (organization_id,),
        )
        roles = [
            {
                "role_id": row["role"],
                "description": row["description"] or "",
            }
            for row in cur.fetchall()
        ]

        return {
            "organization_id": int(organization["org_id"]),
            "name": organization["name"],
            "description": organization["description"] or "",
            "roles": roles,
        }


@router.post("/organizations")
async def http_create_organization(payload: dict[str, Any] = Body(...)):
    return await enqueue_command_response(
        topic=ORG_MANAGEMENT,
        domain="org.management",
        payload={**payload, "action": "CREATE_ORGANIZATION"},
    )


@router.post("/organization-roles")
async def http_create_organization_role(payload: dict[str, Any] = Body(...)):
    return await enqueue_command_response(
        topic=ORG_MANAGEMENT,
        domain="org.management",
        payload={**payload, "action": "CREATE_ORGANIZATION_ROLE"},
    )


@router.post("/organization-tokens")
async def http_create_organization_token(payload: dict[str, Any] = Body(...)):
    return await enqueue_command_response(
        topic=ORG_MANAGEMENT,
        domain="org.management",
        payload={**payload, "action": "CREATE_ORGANIZATION_TOKEN"},
    )


@router.post("/organization-members")
async def http_create_user_in_role(payload: dict[str, Any] = Body(...)):
    return await enqueue_command_response(
        topic=ORG_MANAGEMENT,
        domain="org.management",
        payload={**payload, "action": "CREATE_ORGANIZATION_MEMBER"},
    )


@router.post("/organization-members/join")
async def http_join_organization(payload: dict[str, Any] = Body(...)):
    return await enqueue_command_response(
        topic=ORG_MANAGEMENT,
        domain="org.management",
        payload={**payload, "action": "JOIN_ORGANIZATION"},
    )


@router.post("/organization-members/leave")
async def http_leave_organization(payload: dict[str, Any] = Body(...)):
    return await enqueue_command_response(
        topic=ORG_MANAGEMENT,
        domain="org.management",
        payload={**payload, "action": "LEAVE_ORGANIZATION"},
    )


@router.post("/organization-members/remove")
async def http_remove_organization_member(payload: dict[str, Any] = Body(...)):
    return await enqueue_command_response(
        topic=ORG_MANAGEMENT,
        domain="org.management",
        payload={**payload, "action": "REMOVE_ORGANIZATION_MEMBER"},
    )


@router.post("/organization-token-grants")
async def http_grant_organization_token(payload: dict[str, Any] = Body(...)):
    return await enqueue_command_response(
        topic=ORG_MANAGEMENT,
        domain="org.management",
        payload={**payload, "action": "GRANT_ORGANIZATION_TOKEN"},
    )


@router.post("/organizations/delete")
async def http_delete_organization(payload: dict[str, Any] = Body(...)):
    return await enqueue_command_response(
        topic=ORG_MANAGEMENT,
        domain="org.management",
        payload={**payload, "action": "DELETE_ORGANIZATION"},
    )


@router.put("/organizations/{organization_id}")
async def http_update_organization(
    organization_id: int,
    payload: dict[str, Any] = Body(...),
):
    return await enqueue_command_response(
        topic=ORG_MANAGEMENT,
        domain="org.management",
        payload={
            **payload,
            "organization_id": organization_id,
            "action": "UPDATE_ORGANIZATION",
        },
    )


@router.put("/organization-roles/{organization_id}/{role_id}")
async def http_update_organization_role(
    organization_id: int,
    role_id: str,
    payload: dict[str, Any] = Body(...),
):
    return await enqueue_command_response(
        topic=ORG_MANAGEMENT,
        domain="org.management",
        payload={
            **payload,
            "organization_id": organization_id,
            "role_id": role_id,
            "action": "UPDATE_ORGANIZATION_ROLE",
        },
    )


@router.put("/organization-tokens/{organization_id}/{token_id}")
async def http_update_organization_token(
    organization_id: int,
    token_id: int,
    payload: dict[str, Any] = Body(...),
):
    return await enqueue_command_response(
        topic=ORG_MANAGEMENT,
        domain="org.management",
        payload={
            **payload,
            "organization_id": organization_id,
            "token_id": token_id,
            "action": "UPDATE_ORGANIZATION_TOKEN",
        },
    )


@router.get("/dashboard/users/{user_id}/organizations")
def http_read_user_organizations(user_id: int):
    return _apply(_read_org.read_user_organizations, {"user_id": user_id})


@router.get("/dashboard/users/{user_id}/portfolio")
def http_read_user_portfolio(
    user_id: int,
    cache_mode_name: str = Query("default", alias="cache_mode"),
):
    with cache_mode(cache_mode_name):
        return _read_user_portfolio(user_id)


@router.get("/metadata/policy-options")
def http_read_policy_metadata():
    return _read_policy_metadata()


@router.get("/organizations/{organization_id}")
def http_read_organization(
    organization_id: int,
    user_id: int = Query(...),
    cache_mode_name: str = Query("default", alias="cache_mode"),
):
    return _apply_with_cache_mode(
        _read_org.read_o,
        {"organization_id": organization_id, "user_id": user_id},
        request_cache_mode=cache_mode_name,
    )


@router.get("/organizations/{organization_id}/join-options")
def http_read_org_join_options(organization_id: int):
    return _read_org_join_options(organization_id)


@router.get("/organizations/{organization_id}/events")
def http_read_org_events(
    organization_id: int,
    user_id: int = Query(...),
    cache_mode_name: str = Query("default", alias="cache_mode"),
):
    return _apply_with_cache_mode(
        _read_event.read_o_events,
        {"organization_id": organization_id, "user_id": user_id},
        request_cache_mode=cache_mode_name,
    )


@router.post("/events")
async def http_create_event(payload: dict[str, Any] = Body(...)):
    return await enqueue_command_response(
        topic=EVENT_LIFECYCLE,
        domain="event.lifecycle",
        payload={**payload, "action": "CREATE_EVENT"},
    )


@router.post("/events/designate-token")
async def http_designate_event_token(payload: dict[str, Any] = Body(...)):
    return await enqueue_command_response(
        topic=EVENT_LIFECYCLE,
        domain="event.lifecycle",
        payload={**payload, "action": "DESIGNATE_EVENT_TOKEN"},
    )


@router.post("/events/designate-market-creator")
async def http_designate_event_market_creator(payload: dict[str, Any] = Body(...)):
    return await enqueue_command_response(
        topic=EVENT_LIFECYCLE,
        domain="event.lifecycle",
        payload={**payload, "action": "DESIGNATE_EVENT_MARKET_CREATOR"},
    )


@router.post("/events/designate-constraint")
async def http_designate_event_constraint(payload: dict[str, Any] = Body(...)):
    return await enqueue_command_response(
        topic=EVENT_LIFECYCLE,
        domain="event.lifecycle",
        payload={**payload, "action": "DESIGNATE_EVENT_CONSTRAINT"},
    )


@router.post("/events/designate-open-to")
async def http_designate_event_open_to(payload: dict[str, Any] = Body(...)):
    return await enqueue_command_response(
        topic=EVENT_LIFECYCLE,
        domain="event.lifecycle",
        payload={**payload, "action": "DESIGNATE_EVENT_OPEN_TO"},
    )


@router.post("/events/close")
async def http_designate_event_closed(payload: dict[str, Any] = Body(...)):
    return await enqueue_command_response(
        topic=EVENT_LIFECYCLE,
        domain="event.lifecycle",
        payload={**payload, "action": "DESIGNATE_EVENT_CLOSED"},
    )


@router.post("/events/delete")
async def http_delete_event(payload: dict[str, Any] = Body(...)):
    return await enqueue_command_response(
        topic=EVENT_LIFECYCLE,
        domain="event.lifecycle",
        payload={**payload, "action": "DELETE_EVENT"},
    )


@router.put("/events/{event_id}")
async def http_update_event(
    event_id: int,
    payload: dict[str, Any] = Body(...),
):
    return await enqueue_command_response(
        topic=EVENT_LIFECYCLE,
        domain="event.lifecycle",
        payload={**payload, "event_id": event_id, "action": "UPDATE_EVENT"},
    )


@router.get("/events/{event_id}")
def http_read_event(
    event_id: int,
    user_id: int = Query(...),
    cache_mode_name: str = Query("default", alias="cache_mode"),
):
    return _apply_with_cache_mode(
        _read_event.read_e,
        {"event_id": event_id, "user_id": user_id},
        request_cache_mode=cache_mode_name,
    )


@router.get("/events/{event_id}/markets")
def http_read_event_markets(
    event_id: int,
    user_id: int = Query(...),
    cache_mode_name: str = Query("default", alias="cache_mode"),
):
    return _apply_with_cache_mode(
        _read_market.read_e_markets,
        {"event_id": event_id, "user_id": user_id},
        request_cache_mode=cache_mode_name,
    )


@router.post("/markets")
async def http_create_market(payload: dict[str, Any] = Body(...)):
    return await enqueue_command_response(
        topic=MARKET_OPERATIONS,
        domain="market.operations",
        payload={**payload, "action": "CREATE_MARKET"},
    )


@router.post("/markets/designate-token")
async def http_designate_market_token(payload: dict[str, Any] = Body(...)):
    return await enqueue_command_response(
        topic=MARKET_OPERATIONS,
        domain="market.operations",
        payload={**payload, "action": "DESIGNATE_MARKET_TOKEN"},
    )


@router.post("/markets/designate-result")
async def http_designate_market_result(payload: dict[str, Any] = Body(...)):
    return await enqueue_command_response(
        topic=MARKET_OPERATIONS,
        domain="market.operations",
        payload={**payload, "action": "DESIGNATE_MARKET_RESULT"},
    )


@router.post("/markets/designate-constraint")
async def http_designate_market_constraint(payload: dict[str, Any] = Body(...)):
    return await enqueue_command_response(
        topic=MARKET_OPERATIONS,
        domain="market.operations",
        payload={**payload, "action": "DESIGNATE_MARKET_CONSTRAINT"},
    )


@router.post("/markets/designate-open-to-as")
async def http_designate_market_open_to_as(payload: dict[str, Any] = Body(...)):
    return await enqueue_command_response(
        topic=MARKET_OPERATIONS,
        domain="market.operations",
        payload={**payload, "action": "DESIGNATE_MARKET_OPEN_TO_AS"},
    )


@router.post("/markets/transactions")
async def http_market_transaction(payload: dict[str, Any] = Body(...)):
    return await enqueue_command_response(
        topic=MARKET_OPERATIONS,
        domain="market.operations",
        payload={**payload, "action": "MARKET_TRANSACTION"},
    )


@router.post("/markets/payout")
async def http_market_payout(payload: dict[str, Any] = Body(...)):
    return await enqueue_command_response(
        topic=MARKET_OPERATIONS,
        domain="market.operations",
        payload={**payload, "action": "MARKET_PAYOUT"},
    )


@router.put("/markets/{market_id}")
async def http_update_market(
    market_id: int,
    payload: dict[str, Any] = Body(...),
):
    return await enqueue_command_response(
        topic=MARKET_OPERATIONS,
        domain="market.operations",
        payload={**payload, "market_id": market_id, "action": "UPDATE_MARKET"},
    )


@router.get("/markets/stats/liquidity")
def http_stats_liquidity(
    user_id: int = Query(...),
    market_id: int = Query(...),
    cache_mode_name: str = Query("default", alias="cache_mode"),
):
    return _apply_with_cache_mode(
        _read_market.stats_m_liquidity,
        {"user_id": user_id, "market_id": market_id},
        request_cache_mode=cache_mode_name,
    )


@router.get("/markets/quote")
def http_market_quote(
    user_id: int = Query(...),
    market_id: int = Query(...),
    token_id: int = Query(...),
    side: bool = Query(...),
    qty: int = Query(..., ge=1),
    transaction_type: str = Query(...),
    cache_mode_name: str = Query("default", alias="cache_mode"),
):
    return _apply_with_cache_mode(
        _read_market.quote_m,
        {
            "user_id": user_id,
            "market_id": market_id,
            "token_id": token_id,
            "side": side,
            "qty": qty,
            "transaction_type": transaction_type,
        },
        request_cache_mode=cache_mode_name,
    )


@router.get("/markets/stats/time-focus")
def http_stats_time_focus(
    user_id: int = Query(...),
    market_id: int = Query(...),
    cache_mode_name: str = Query("default", alias="cache_mode"),
):
    return _apply_with_cache_mode(
        _read_market.stats_m_time_focus,
        {"user_id": user_id, "market_id": market_id},
        request_cache_mode=cache_mode_name,
    )


@router.get("/markets/stats/whales")
def http_stats_whales(
    user_id: int = Query(...),
    market_id: int = Query(...),
    cache_mode_name: str = Query("default", alias="cache_mode"),
):
    return _apply_with_cache_mode(
        _read_market.stats_m_whales,
        {"user_id": user_id, "market_id": market_id},
        request_cache_mode=cache_mode_name,
    )


@router.get("/markets/stats/trade-distribution")
def http_stats_trade_distribution(
    user_id: int = Query(...),
    market_id: int = Query(...),
    cache_mode_name: str = Query("default", alias="cache_mode"),
):
    return _apply_with_cache_mode(
        _read_market.stats_m_trade_distribution,
        {"user_id": user_id, "market_id": market_id},
        request_cache_mode=cache_mode_name,
    )


@router.get("/markets/stats/window-comparison")
def http_stats_window_comparison(
    user_id: int = Query(...),
    market_id: int = Query(...),
    hours: int = Query(24, ge=1),
    cache_mode_name: str = Query("default", alias="cache_mode"),
):
    return _apply_with_cache_mode(
        _read_market.stats_m_window_comparison,
        {"user_id": user_id, "market_id": market_id, "hours": hours},
        request_cache_mode=cache_mode_name,
    )


@router.get("/markets/points")
def http_market_points(
    user_id: int = Query(...),
    market_id: int = Query(...),
    span: int = Query(..., ge=1),
    hours: int | None = Query(None, ge=1),
    cache_mode_name: str = Query("default", alias="cache_mode"),
):
    return _apply_with_cache_mode(
        _read_market.points_m,
        {"user_id": user_id, "market_id": market_id, "span": span, "hours": hours},
        request_cache_mode=cache_mode_name,
    )


@router.get("/markets/{market_id}")
def http_read_market(
    market_id: int,
    user_id: int = Query(...),
    cache_mode_name: str = Query("default", alias="cache_mode"),
):
    return _apply_with_cache_mode(
        _read_market.read_m,
        {"market_id": market_id, "user_id": user_id},
        request_cache_mode=cache_mode_name,
    )


@router.post("/users/signup")
async def http_user_signup(payload: dict[str, Any] = Body(...)):
    return await enqueue_command_response(
        topic=USER_ACCOUNT,
        domain="user.account",
        payload={**payload, "action": "USER_SIGNUP"},
    )


@router.post("/users/login")
async def http_user_login(payload: dict[str, Any] = Body(...)):
    return await enqueue_command_response(
        topic=USER_ACCOUNT,
        domain="user.account",
        payload={**payload, "action": "USER_LOGIN"},
    )


@router.post("/users/logout")
async def http_user_logout(payload: dict[str, Any] = Body(...)):
    return await enqueue_command_response(
        topic=USER_ACCOUNT,
        domain="user.account",
        payload={**payload, "action": "USER_LOGOUT"},
    )


@router.post("/auth/login")
def http_auth_login(payload: dict[str, Any] = Body(...)):
    body = {
        "username": payload.get("username", payload.get("email", payload.get("login_identifier"))),
        "password": payload.get("password"),
    }
    return _apply(_write_user.user_login, body)


@router.put("/users/{user_id}/profile")
async def http_update_user_profile(
    user_id: int,
    payload: dict[str, Any] = Body(...),
):
    body = dict(payload)
    body.setdefault("user_id", user_id)
    body.setdefault("target_user_id", user_id)
    return await enqueue_command_response(
        topic=USER_ACCOUNT,
        domain="user.account",
        payload={**body, "action": "UPDATE_USER_PROFILE"},
    )


@router.put("/users/{user_id}/password")
async def http_update_user_password(
    user_id: int,
    payload: dict[str, Any] = Body(...),
):
    return await enqueue_command_response(
        topic=USER_ACCOUNT,
        domain="user.account",
        payload={**payload, "user_id": user_id, "action": "UPDATE_USER_PASSWORD"},
    )
