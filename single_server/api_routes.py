"""HTTP handlers that call backend functions with a MySQL connection + cursor."""

from __future__ import annotations

from typing import Any

import pymysql
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from database import get_db

from event_logic import (
    create_e,
    designate_e_closed,
    designate_e_contraint,
    designate_e_market_creator,
    designate_e_open_to,
    designate_e_token,
)
from market_logic import (
    create_m,
    designate_m_contraint,
    designate_m_open_to_as,
    designate_m_result,
    designate_m_token,
    do_m_payout,
    do_m_transaction,
    points_m,
    stats_m_liquidity,
    stats_m_trade_distribution,
    stats_m_time_focus,
    stats_m_window_comparison,
    stats_m_whales,
)
from organization import create_o, create_o_role, create_o_token
# from trading import Handle_Buy, Handle_Sell
from user_authenticity import user_login, user_logout, user_signup

router = APIRouter()


def _call_db(conn, fn, *args: Any):
    """This helper only
    handles unexpected **MySQL driver** errors (rollback + 500).
    """
    cur = conn.cursor()
    try:
        return fn(cur, conn, *args)
    except pymysql.err.Error as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        cur.close()


_DOMAIN_HTTP_STATUS = {
    "permission": 403,
    "duplicate": 409,
    "validation": 422,
    "precondition": 400,
    "not_open": 409,
    "not_closed": 409,
    "auth": 401,
}


def _unwrap_result(result: Any) -> dict[str, Any]:
    """event_logic / market_logic return {ok, ...} dicts; translate failures to HTTP errors."""
    if not isinstance(result, dict) or result.get("ok") is not False:
        return result
    err = result.get("error", "unknown")
    msg = result.get("message", "Request failed")
    status = _DOMAIN_HTTP_STATUS.get(err, 400)
    raise HTTPException(status_code=status, detail=msg)


# --- Organization ---


class OrgCreateBody(BaseModel):
    user_id: int
    name: str
    description: str


class OrgRoleCreateBody(BaseModel):
    user_id: int
    organization_id: int
    name: str
    desc: str


class OrgTokenCreateBody(BaseModel):
    user_id: int
    organization_id: int
    token_name: str
    description: str | None = None


@router.post("/organizations")
def http_create_organization(body: OrgCreateBody, conn=Depends(get_db)):
    return _unwrap_result(
        _call_db(conn, create_o, body.user_id, body.name, body.description)
    )


@router.post("/organization-roles")
def http_create_organization_role(body: OrgRoleCreateBody, conn=Depends(get_db)):
    return _unwrap_result(
        _call_db(
            conn, create_o_role, body.user_id, body.organization_id, body.name, body.desc
        )
    )


@router.post("/organization-tokens")
def http_create_organization_token(body: OrgTokenCreateBody, conn=Depends(get_db)):
    return _unwrap_result(
        _call_db(
            conn,
            create_o_token,
            body.user_id,
            body.organization_id,
            body.token_name,
            body.description,
        )
    )


# --- Events ---


class EventCreateBody(BaseModel):
    user_id: int
    organization_id: int
    caption: str


class EventTokenBody(BaseModel):
    user_id: int
    event_id: int
    token_id: int


class EventMarketCreatorBody(BaseModel):
    user_id: int
    event_id: int
    market_creator_id: int


class EventConstraintBody(BaseModel):
    user_id: int
    event_id: int
    constraint_id: int
    value: int


class EventOpenToBody(BaseModel):
    user_id: int
    event_id: int
    role_id: str


class EventCloseBody(BaseModel):
    user_id: int
    event_id: int


@router.post("/events")
def http_create_event(body: EventCreateBody, conn=Depends(get_db)):
    return _unwrap_result(
        _call_db(conn, create_e, body.user_id, body.organization_id, body.caption)
    )


@router.post("/events/designate-token")
def http_designate_event_token(body: EventTokenBody, conn=Depends(get_db)):
    return _unwrap_result(
        _call_db(conn, designate_e_token, body.user_id, body.event_id, body.token_id)
    )


@router.post("/events/designate-market-creator")
def http_designate_event_market_creator(body: EventMarketCreatorBody, conn=Depends(get_db)):
    return _unwrap_result(
        _call_db(
            conn,
            designate_e_market_creator,
            body.user_id,
            body.event_id,
            body.market_creator_id,
        )
    )


@router.post("/events/designate-constraint")
def http_designate_event_constraint(body: EventConstraintBody, conn=Depends(get_db)):
    return _unwrap_result(
        _call_db(
            conn,
            designate_e_contraint,
            body.user_id,
            body.event_id,
            body.constraint_id,
            body.value,
        )
    )


@router.post("/events/designate-open-to")
def http_designate_event_open_to(body: EventOpenToBody, conn=Depends(get_db)):
    return _unwrap_result(
        _call_db(
            conn, designate_e_open_to, body.user_id, body.event_id, body.role_id
        )
    )


@router.post("/events/close")
def http_designate_event_closed(body: EventCloseBody, conn=Depends(get_db)):
    return _unwrap_result(
        _call_db(conn, designate_e_closed, body.user_id, body.event_id)
    )


# --- Markets ---


class MarketCreateBody(BaseModel):
    event_id: int
    question: str
    user_id: int


class MarketTokenBody(BaseModel):
    user_id: int
    market_id: int
    token_id: int


class MarketResultBody(BaseModel):
    user_id: int
    market_id: int
    result: bool


class MarketConstraintBody(BaseModel):
    user_id: int
    market_id: int
    constraint_id: int
    value: int


class MarketOpenToAsBody(BaseModel):
    user_id: int
    market_id: int
    role_id: str
    as_id: str


class MarketTransactionBody(BaseModel):
    user_id: int
    market_id: int
    token_id: int
    transaction_id: int
    transaction_type: str = Field(description="Trade direction stored as type in market_transaction: BUY or SELL.")
    side: bool
    qty: int


class MarketPayoutBody(BaseModel):
    user_id: int
    market_id: int
    token_id: int


@router.post("/markets")
def http_create_market(body: MarketCreateBody, conn=Depends(get_db)):
    return _unwrap_result(
        _call_db(conn, create_m, body.user_id, body.event_id, body.question, "")
    )


@router.post("/markets/designate-token")
def http_designate_market_token(body: MarketTokenBody, conn=Depends(get_db)):
    return _unwrap_result(
        _call_db(conn, designate_m_token, body.user_id, body.market_id, body.token_id)
    )


@router.post("/markets/designate-result")
def http_designate_market_result(body: MarketResultBody, conn=Depends(get_db)):
    return _unwrap_result(
        _call_db(conn, designate_m_result, body.user_id, body.market_id, body.result)
    )


@router.post("/markets/designate-constraint")
def http_designate_market_constraint(body: MarketConstraintBody, conn=Depends(get_db)):
    return _unwrap_result(
        _call_db(
            conn,
            designate_m_contraint,
            body.user_id,
            body.market_id,
            body.constraint_id,
            body.value,
        )
    )


@router.post("/markets/designate-open-to-as")
def http_designate_market_open_to_as(body: MarketOpenToAsBody, conn=Depends(get_db)):
    return _unwrap_result(
        _call_db(
            conn,
            designate_m_open_to_as,
            body.user_id,
            body.market_id,
            body.role_id,
            body.as_id,
        )
    )


@router.post("/markets/transactions")
def http_market_transaction(body: MarketTransactionBody, conn=Depends(get_db)):
    return _unwrap_result(
        _call_db(
            conn,
            do_m_transaction,
            body.user_id,
            body.market_id,
            body.token_id,
            body.side,
            body.qty,
            body.transaction_id,
            body.transaction_type,
        )
    )


@router.post("/markets/payout")
def http_market_payout(body: MarketPayoutBody, conn=Depends(get_db)):
    return _unwrap_result(
        _call_db(conn, do_m_payout, body.user_id, body.market_id, body.token_id)
    )


@router.get("/markets/stats/liquidity")
def http_stats_liquidity(
    user_id: int = Query(...),
    market_id: int = Query(...),
    conn=Depends(get_db),
):
    return _unwrap_result(_call_db(conn, stats_m_liquidity, user_id, market_id))


@router.get("/markets/stats/time-focus")
def http_stats_time_focus(
    user_id: int = Query(...),
    market_id: int = Query(...),
    conn=Depends(get_db),
):
    return _unwrap_result(_call_db(conn, stats_m_time_focus, user_id, market_id))


@router.get("/markets/stats/whales")
def http_stats_whales(
    user_id: int = Query(...),
    market_id: int = Query(...),
    conn=Depends(get_db),
):
    return _unwrap_result(_call_db(conn, stats_m_whales, user_id, market_id))


@router.get("/markets/stats/trade-distribution")
def http_stats_trade_distribution(
    user_id: int = Query(...),
    market_id: int = Query(...),
    conn=Depends(get_db),
):
    return _unwrap_result(_call_db(conn, stats_m_trade_distribution, user_id, market_id))


@router.get("/markets/stats/window-comparison")
def http_stats_window_comparison(
    user_id: int = Query(...),
    market_id: int = Query(...),
    hours: int = Query(24, ge=1),
    conn=Depends(get_db),
):
    return _unwrap_result(_call_db(conn, stats_m_window_comparison, user_id, market_id, hours))


@router.get("/markets/points")
def http_market_points(
    user_id: int = Query(...),
    market_id: int = Query(...),
    span: int = Query(..., ge=1),
    conn=Depends(get_db),
):
    return _unwrap_result(_call_db(conn, points_m, user_id, market_id, span))


# --- Trading (in-memory demo) ---


class TradingOrderBody(BaseModel):
    request_id: str
    user_id: str
    market_id: int
    quantity: int
    price_limit_cents: int


# @router.post("/trading/buy")
# def http_trading_buy(body: TradingOrderBody, conn=Depends(get_db)):
#     ok = _call_db(conn, Handle_Buy, body.request_id, body.user_id, body.market_id, body.quantity, body.price_limit_cents)
#     return {"ok": bool(ok)}


# @router.post("/trading/sell")
# def http_trading_sell(body: TradingOrderBody, conn=Depends(get_db)):
#     ok = _call_db(conn, Handle_Sell, body.request_id, body.user_id, body.market_id, body.quantity, body.price_limit_cents)
#     return {"ok": bool(ok)}


# --- Auth (user_authenticity) ---

class SignupBody(BaseModel):
    first: str
    last: str
    email: str
    username: str
    password: str
    age: int | None = None

class LoginBody(BaseModel):
    username: str
    password: str


class LogoutBody(BaseModel):
    session_token: str


@router.post("/auth/login")
def http_login(body: LoginBody, conn=Depends(get_db)):
    return _unwrap_result(
        _call_db(conn, user_login, body.username, body.password)
    )


@router.post("/auth/signup")
def http_signup(body: SignupBody, conn=Depends(get_db)):
    return _unwrap_result(
        _call_db(
            conn,
            user_signup,
            body.first,
            body.last,
            body.email,
            body.username,
            body.password,
            body.age,
        )
    )


@router.post("/auth/logout")
def http_logout(body: LogoutBody, conn=Depends(get_db)):
    return _unwrap_result(_call_db(conn, user_logout, body.session_token))
