"""Synchronous handlers for Kafka → MySQL, delegating to ``Backend Functions`` write modules."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

_bf_root = Path(__file__).resolve().parent / "Backend Functions"
for _dir in (
    _bf_root,
    _bf_root / "Organization Functions",
    _bf_root / "Event Functions",
    _bf_root / "Market Functions",
    _bf_root / "User Functions",
):
    s = str(_dir)
    if s not in sys.path:
        sys.path.insert(0, s)


def _load_bf_module(unique_name: str, file_path: Path):
    spec = importlib.util.spec_from_file_location(unique_name, file_path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[unique_name] = mod
    spec.loader.exec_module(mod)
    return mod


_write_org = _load_bf_module(
    "polaris_bf_write_organization",
    _bf_root / "Organization Functions" / "Write_Organization.py",
)
_write_event = _load_bf_module(
    "polaris_bf_write_event",
    _bf_root / "Event Functions" / "Write_Event.py",
)
_write_market = _load_bf_module(
    "polaris_bf_write_market",
    _bf_root / "Market Functions" / "Write_Market.py",
)
_write_user = _load_bf_module(
    "polaris_bf_write_user",
    _bf_root / "User Functions" / "Write_User.py",
)
_update_user = _load_bf_module(
    "polaris_bf_update_user",
    _bf_root / "User Functions" / "Update_User.py",
)


def _raise_if_failed(result: Any) -> None:
    if isinstance(result, dict) and result.get("ok") is False:
        raise ValueError(result.get("message", str(result)))


def sync_create_o(data: dict[str, Any]) -> None:
    _raise_if_failed(_write_org.create_o(data))


def sync_create_o_role(data: dict[str, Any]) -> None:
    _raise_if_failed(_write_org.create_o_role(data))


def sync_create_o_token(data: dict[str, Any]) -> None:
    _raise_if_failed(_write_org.create_o_token(data))


def sync_create_e(data: dict[str, Any]) -> None:
    _raise_if_failed(_write_event.create_e(data))


def sync_designate_e_token(data: dict[str, Any]) -> None:
    _raise_if_failed(_write_event.designate_e_token(data))


def sync_designate_e_market_creator(data: dict[str, Any]) -> None:
    _raise_if_failed(_write_event.designate_e_market_creator(data))


def sync_designate_e_contraint(data: dict[str, Any]) -> None:
    _raise_if_failed(_write_event.designate_e_contraint(data))


def sync_designate_e_open_to(data: dict[str, Any]) -> None:
    _raise_if_failed(_write_event.designate_e_open_to(data))


def sync_designate_e_closed(data: dict[str, Any]) -> None:
    _raise_if_failed(_write_event.designate_e_closed(data))


def sync_create_m(data: dict[str, Any]) -> None:
    _raise_if_failed(_write_market.create_m(data))


def sync_designate_m_token(data: dict[str, Any]) -> None:
    _raise_if_failed(_write_market.designate_m_token(data))


def sync_designate_m_result(data: dict[str, Any]) -> None:
    _raise_if_failed(_write_market.designate_m_result(data))


def sync_designate_m_contraint(data: dict[str, Any]) -> None:
    _raise_if_failed(_write_market.designate_m_contraint(data))


def sync_designate_m_open_to_as(data: dict[str, Any]) -> None:
    _raise_if_failed(_write_market.designate_m_open_to_as(data))


def sync_do_m_transaction(data: dict[str, Any]) -> None:
    _raise_if_failed(_write_market.do_m_transaction(data))


def sync_do_m_payout(data: dict[str, Any]) -> None:
    _raise_if_failed(_write_market.do_m_payout(data))


def sync_user_account_message(data: dict[str, Any]) -> None:
    """Apply user account payloads through the existing User backend functions."""
    action = data.get("action")
    if action == "USER_SIGNUP":
        _raise_if_failed(_write_user.user_signup(data))
        return
    if action == "USER_LOGIN":
        _raise_if_failed(_write_user.user_login(data))
        return
    if action == "USER_LOGOUT":
        _raise_if_failed(_write_user.user_logout(data))
        return
    if action == "UPDATE_USER_PROFILE":
        _raise_if_failed(_update_user.update_user_profile(data))
        return
    if action == "UPDATE_USER_PASSWORD":
        _raise_if_failed(_update_user.update_user_password(data))
        return
    if action == "TEST_PING":
        return
    raise ValueError(f"unsupported user.account action: {action!r}")
