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
    stats_m_time_focus,
    stats_m_whales,
)
from organization import create_o, create_o_role, create_o_token
from trading import Handle_Buy, Handle_Sell
from user_authenticity import SignupError, user_login, user_logout, user_signup

router = APIRouter()


def _call_db(conn, fn, *args: Any):
    cur = conn.cursor()
    try:
        return fn(cur, conn, *args)
    except pymysql.err.Error as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e)) from e
    finally:
        cur.close()


def _rows_payload(rows: Any) -> dict[str, Any]:
    if rows is None:
        return {"ok": False}
    if isinstance(rows, (list, tuple)):
        return {
            "ok": True,
            "rows": [[*r] if isinstance(r, (list, tuple)) else r for r in rows],
        }
    return {"ok": True, "data": rows}


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
    oid = _call_db(conn, create_o, body.user_id, body.name, body.description)
    if oid is None:
        raise HTTPException(
            status_code=400,
            detail="No row in `users` with this user_id. Add a user to the `users` table first, then use its `id`.",
        )
    return {"ok": True, "organization_id": oid}


@router.post("/organization-roles")
def http_create_organization_role(body: OrgRoleCreateBody, conn=Depends(get_db)):
    rid = _call_db(
        conn, create_o_role, body.user_id, body.organization_id, body.name, body.desc
    )
    if rid is None:
        return {"ok": False}
    return {"ok": True, "role": rid}


@router.post("/organization-tokens")
def http_create_organization_token(body: OrgTokenCreateBody, conn=Depends(get_db)):
    tid = _call_db(
        conn,
        create_o_token,
        body.user_id,
        body.organization_id,
        body.token_name,
        body.description,
    )
    if tid is None:
        return {"ok": False}
    return {"ok": True, "token_id": tid}


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
    eid = _call_db(
        conn, create_e, body.user_id, body.organization_id, body.caption
    )
    if eid is None:
        return {"ok": False}
    return {"ok": True, "event_id": eid}


@router.post("/events/designate-token")
def http_designate_event_token(body: EventTokenBody, conn=Depends(get_db)):
    ok = _call_db(
        conn, designate_e_token, body.user_id, body.event_id, body.token_id
    )
    return {"ok": bool(ok)}


@router.post("/events/designate-market-creator")
def http_designate_event_market_creator(body: EventMarketCreatorBody, conn=Depends(get_db)):
    ok = _call_db(
        conn,
        designate_e_market_creator,
        body.user_id,
        body.event_id,
        body.market_creator_id,
    )
    return {"ok": bool(ok)}


@router.post("/events/designate-constraint")
def http_designate_event_constraint(body: EventConstraintBody, conn=Depends(get_db)):
    ok = _call_db(
        conn,
        designate_e_contraint,
        body.user_id,
        body.event_id,
        body.constraint_id,
        body.value,
    )
    return {"ok": bool(ok)}


@router.post("/events/designate-open-to")
def http_designate_event_open_to(body: EventOpenToBody, conn=Depends(get_db)):
    ok = _call_db(
        conn, designate_e_open_to, body.user_id, body.event_id, body.role_id
    )
    return {"ok": bool(ok)}


@router.post("/events/close")
def http_designate_event_closed(body: EventCloseBody, conn=Depends(get_db)):
    ok = _call_db(conn, designate_e_closed, body.user_id, body.event_id)
    return {"ok": bool(ok)}


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
    price: int = Field(description="Stored as price in market_transaction (param `type` in backend).")
    side: bool
    qty: int


class MarketPayoutBody(BaseModel):
    user_id: int
    market_id: int
    token_id: int


@router.post("/markets")
def http_create_market(body: MarketCreateBody, conn=Depends(get_db)):
    new_id = _call_db(
        conn, create_m, body.user_id, body.event_id, body.question, ""
    )
    if new_id is None:
        return {"ok": False}
    return {"ok": True, "market_id": new_id}


@router.post("/markets/designate-token")
def http_designate_market_token(body: MarketTokenBody, conn=Depends(get_db)):
    ok = _call_db(
        conn, designate_m_token, body.user_id, body.market_id, body.token_id
    )
    return {"ok": bool(ok)}


@router.post("/markets/designate-result")
def http_designate_market_result(body: MarketResultBody, conn=Depends(get_db)):
    ok = _call_db(
        conn, designate_m_result, body.user_id, body.market_id, body.result
    )
    return {"ok": bool(ok)}


@router.post("/markets/designate-constraint")
def http_designate_market_constraint(body: MarketConstraintBody, conn=Depends(get_db)):
    ok = _call_db(
        conn,
        designate_m_contraint,
        body.user_id,
        body.market_id,
        body.constraint_id,
        body.value,
    )
    return {"ok": bool(ok)}


@router.post("/markets/designate-open-to-as")
def http_designate_market_open_to_as(body: MarketOpenToAsBody, conn=Depends(get_db)):
    ok = _call_db(
        conn,
        designate_m_open_to_as,
        body.user_id,
        body.market_id,
        body.role_id,
        body.as_id,
    )
    return {"ok": bool(ok)}


@router.post("/markets/transactions")
def http_market_transaction(body: MarketTransactionBody, conn=Depends(get_db)):
    tid = _call_db(
        conn,
        do_m_transaction,
        body.user_id,
        body.market_id,
        body.token_id,
        body.price,
        body.side,
        body.qty,
    )
    if tid is None:
        return {"ok": False}
    return {"ok": True, "transaction_id": tid}


@router.post("/markets/payout")
def http_market_payout(body: MarketPayoutBody, conn=Depends(get_db)):
    ok = _call_db(conn, do_m_payout, body.user_id, body.market_id, body.token_id)
    return {"ok": bool(ok)}


@router.get("/markets/stats/liquidity")
def http_stats_liquidity(
    user_id: int = Query(...),
    market_id: int = Query(...),
    conn=Depends(get_db),
):
    rows = _call_db(conn, stats_m_liquidity, user_id, market_id)
    return _rows_payload(rows)


@router.get("/markets/stats/time-focus")
def http_stats_time_focus(
    user_id: int = Query(...),
    market_id: int = Query(...),
    conn=Depends(get_db),
):
    rows = _call_db(conn, stats_m_time_focus, user_id, market_id)
    return _rows_payload(rows)


@router.get("/markets/stats/whales")
def http_stats_whales(
    user_id: int = Query(...),
    market_id: int = Query(...),
    conn=Depends(get_db),
):
    rows = _call_db(conn, stats_m_whales, user_id, market_id)
    return _rows_payload(rows)


@router.get("/markets/points")
def http_market_points(
    user_id: int = Query(...),
    market_id: int = Query(...),
    span: int = Query(..., ge=1),
    conn=Depends(get_db),
):
    rows = _call_db(conn, points_m, user_id, market_id, span)
    return _rows_payload(rows)


# --- Trading (in-memory demo) ---


class TradingOrderBody(BaseModel):
    request_id: str
    user_id: str
    market_id: int
    quantity: int
    price_limit_cents: int


@router.post("/trading/buy")
def http_trading_buy(body: TradingOrderBody):
    return Handle_Buy(
        body.request_id,
        body.user_id,
        body.market_id,
        body.quantity,
        body.price_limit_cents,
    )


@router.post("/trading/sell")
def http_trading_sell(body: TradingOrderBody):
    return Handle_Sell(
        body.request_id,
        body.user_id,
        body.market_id,
        body.quantity,
        body.price_limit_cents,
    )


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
def http_login(body: LoginBody):
    try:
        token = user_login(body.username, body.password)
        return {"ok": True, "session_token": token}
    except Exception as e:
        raise HTTPException(status_code=401, detail=str(e)) from e


@router.post("/auth/signup")
def http_signup(body: SignupBody, conn=Depends(get_db)):
    try:
        user_id = _call_db(
            conn,
            user_signup,
            body.first,
            body.last,
            body.email,
            body.username,
            body.password,
            body.age,
        )
        return {"ok": True, "user_id": user_id}
    except SignupError as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail) from e
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/auth/logout")
def http_logout(body: LogoutBody):
    try:
        user_logout(body.session_token)
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
