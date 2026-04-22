"""Direct HTTP entrypoints for backend functions under ``core/Backend Functions``."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Body, HTTPException, Query

from .read_cache import cache_mode

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


@router.post("/organizations")
def http_create_organization(payload: dict[str, Any] = Body(...)):
    return _apply(_write_org.create_o, payload)


@router.post("/organization-roles")
def http_create_organization_role(payload: dict[str, Any] = Body(...)):
    return _apply(_write_org.create_o_role, payload)


@router.post("/organization-tokens")
def http_create_organization_token(payload: dict[str, Any] = Body(...)):
    return _apply(_write_org.create_o_token, payload)


@router.post("/organization-members")
def http_create_user_in_role(payload: dict[str, Any] = Body(...)):
    return _apply(_write_org.create_user_in_role, payload)


@router.put("/organizations/{organization_id}")
def http_update_organization(
    organization_id: int,
    payload: dict[str, Any] = Body(...),
):
    return _apply(_update_org.update_o, {**payload, "organization_id": organization_id})


@router.put("/organization-roles/{organization_id}/{role_id}")
def http_update_organization_role(
    organization_id: int,
    role_id: str,
    payload: dict[str, Any] = Body(...),
):
    return _apply(
        _update_org.update_o_role,
        {**payload, "organization_id": organization_id, "role_id": role_id},
    )


@router.put("/organization-tokens/{organization_id}/{token_id}")
def http_update_organization_token(
    organization_id: int,
    token_id: int,
    payload: dict[str, Any] = Body(...),
):
    return _apply(
        _update_org.update_o_token,
        {**payload, "organization_id": organization_id, "token_id": token_id},
    )


@router.get("/dashboard/users/{user_id}/organizations")
def http_read_user_organizations(user_id: int):
    return _apply(_read_org.read_user_organizations, {"user_id": user_id})


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
def http_create_event(payload: dict[str, Any] = Body(...)):
    return _apply(_write_event.create_e, payload)


@router.post("/events/designate-token")
def http_designate_event_token(payload: dict[str, Any] = Body(...)):
    return _apply(_write_event.designate_e_token, payload)


@router.post("/events/designate-market-creator")
def http_designate_event_market_creator(payload: dict[str, Any] = Body(...)):
    return _apply(_write_event.designate_e_market_creator, payload)


@router.post("/events/designate-constraint")
def http_designate_event_constraint(payload: dict[str, Any] = Body(...)):
    return _apply(_write_event.designate_e_contraint, payload)


@router.post("/events/designate-open-to")
def http_designate_event_open_to(payload: dict[str, Any] = Body(...)):
    return _apply(_write_event.designate_e_open_to, payload)


@router.post("/events/close")
def http_designate_event_closed(payload: dict[str, Any] = Body(...)):
    return _apply(_write_event.designate_e_closed, payload)


@router.put("/events/{event_id}")
def http_update_event(
    event_id: int,
    payload: dict[str, Any] = Body(...),
):
    return _apply(_update_event.update_e, {**payload, "event_id": event_id})


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
def http_create_market(payload: dict[str, Any] = Body(...)):
    return _apply(_write_market.create_m, payload)


@router.post("/markets/designate-token")
def http_designate_market_token(payload: dict[str, Any] = Body(...)):
    return _apply(_write_market.designate_m_token, payload)


@router.post("/markets/designate-result")
def http_designate_market_result(payload: dict[str, Any] = Body(...)):
    return _apply(_write_market.designate_m_result, payload)


@router.post("/markets/designate-constraint")
def http_designate_market_constraint(payload: dict[str, Any] = Body(...)):
    return _apply(_write_market.designate_m_contraint, payload)


@router.post("/markets/designate-open-to-as")
def http_designate_market_open_to_as(payload: dict[str, Any] = Body(...)):
    return _apply(_write_market.designate_m_open_to_as, payload)


@router.post("/markets/transactions")
def http_market_transaction(payload: dict[str, Any] = Body(...)):
    return _apply(_write_market.do_m_transaction, payload)


@router.post("/markets/payout")
def http_market_payout(payload: dict[str, Any] = Body(...)):
    return _apply(_write_market.do_m_payout, payload)


@router.put("/markets/{market_id}")
def http_update_market(
    market_id: int,
    payload: dict[str, Any] = Body(...),
):
    return _apply(_update_market.update_m, {**payload, "market_id": market_id})


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
    cache_mode_name: str = Query("default", alias="cache_mode"),
):
    return _apply_with_cache_mode(
        _read_market.points_m,
        {"user_id": user_id, "market_id": market_id, "span": span},
        request_cache_mode=cache_mode_name,
    )


@router.post("/users/signup")
def http_user_signup(payload: dict[str, Any] = Body(...)):
    return _apply(_write_user.user_signup, payload)


@router.post("/users/login")
def http_user_login(payload: dict[str, Any] = Body(...)):
    return _apply(_write_user.user_login, payload)


@router.post("/users/logout")
def http_user_logout(payload: dict[str, Any] = Body(...)):
    return _apply(_write_user.user_logout, payload)


@router.put("/users/{user_id}/profile")
def http_update_user_profile(
    user_id: int,
    payload: dict[str, Any] = Body(...),
):
    body = dict(payload)
    body.setdefault("user_id", user_id)
    body.setdefault("target_user_id", user_id)
    return _apply(_update_user.update_user_profile, body)


@router.put("/users/{user_id}/password")
def http_update_user_password(
    user_id: int,
    payload: dict[str, Any] = Body(...),
):
    return _apply(_update_user.update_user_password, {**payload, "user_id": user_id})
