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
_update_org = _load_bf_module(
    "polaris_bf_update_organization",
    _bf_root / "Organization Functions" / "Update_Organization.py",
)
_update_event = _load_bf_module(
    "polaris_bf_update_event",
    _bf_root / "Event Functions" / "Update_Event.py",
)
_update_market = _load_bf_module(
    "polaris_bf_update_market",
    _bf_root / "Market Functions" / "Update_Market.py",
)
_update_user = _load_bf_module(
    "polaris_bf_update_user",
    _bf_root / "User Functions" / "Update_User.py",
)


def _raise_if_failed(result: Any) -> Any:
    if isinstance(result, dict) and result.get("ok") is False:
        raise ValueError(result.get("message", str(result)))
    return result


def sync_create_o(data: dict[str, Any]) -> Any:
    return _raise_if_failed(_write_org.create_o(data))


def sync_create_o_role(data: dict[str, Any]) -> Any:
    return _raise_if_failed(_write_org.create_o_role(data))


def sync_create_o_token(data: dict[str, Any]) -> Any:
    return _raise_if_failed(_write_org.create_o_token(data))


def sync_create_user_in_role(data: dict[str, Any]) -> Any:
    return _raise_if_failed(_write_org.create_user_in_role(data))


def sync_join_o(data: dict[str, Any]) -> Any:
    return _raise_if_failed(_write_org.join_o(data))


def sync_leave_o(data: dict[str, Any]) -> Any:
    return _raise_if_failed(_write_org.leave_o(data))


def sync_remove_user_from_o(data: dict[str, Any]) -> Any:
    return _raise_if_failed(_write_org.remove_user_from_o(data))


def sync_grant_o_token_to_user(data: dict[str, Any]) -> Any:
    return _raise_if_failed(_write_org.grant_o_token_to_user(data))


def sync_delete_o(data: dict[str, Any]) -> Any:
    return _raise_if_failed(_write_org.delete_o(data))


def sync_update_o(data: dict[str, Any]) -> Any:
    return _raise_if_failed(_update_org.update_o(data))


def sync_update_o_role(data: dict[str, Any]) -> Any:
    return _raise_if_failed(_update_org.update_o_role(data))


def sync_update_o_token(data: dict[str, Any]) -> Any:
    return _raise_if_failed(_update_org.update_o_token(data))


def sync_create_e(data: dict[str, Any]) -> Any:
    return _raise_if_failed(_write_event.create_e(data))


def sync_designate_e_token(data: dict[str, Any]) -> Any:
    return _raise_if_failed(_write_event.designate_e_token(data))


def sync_designate_e_market_creator(data: dict[str, Any]) -> Any:
    return _raise_if_failed(_write_event.designate_e_market_creator(data))


def sync_designate_e_contraint(data: dict[str, Any]) -> Any:
    return _raise_if_failed(_write_event.designate_e_contraint(data))


def sync_designate_e_open_to(data: dict[str, Any]) -> Any:
    return _raise_if_failed(_write_event.designate_e_open_to(data))


def sync_designate_e_closed(data: dict[str, Any]) -> Any:
    return _raise_if_failed(_write_event.designate_e_closed(data))


def sync_delete_e(data: dict[str, Any]) -> Any:
    return _raise_if_failed(_write_event.delete_e(data))


def sync_update_e(data: dict[str, Any]) -> Any:
    return _raise_if_failed(_update_event.update_e(data))


def sync_create_m(data: dict[str, Any]) -> Any:
    return _raise_if_failed(_write_market.create_m(data))


def sync_designate_m_token(data: dict[str, Any]) -> Any:
    return _raise_if_failed(_write_market.designate_m_token(data))


def sync_designate_m_result(data: dict[str, Any]) -> Any:
    return _raise_if_failed(_write_market.designate_m_result(data))


def sync_designate_m_contraint(data: dict[str, Any]) -> Any:
    return _raise_if_failed(_write_market.designate_m_contraint(data))


def sync_designate_m_open_to_as(data: dict[str, Any]) -> Any:
    return _raise_if_failed(_write_market.designate_m_open_to_as(data))


def sync_do_m_transaction(data: dict[str, Any]) -> Any:
    return _raise_if_failed(_write_market.do_m_transaction(data))


def sync_do_m_payout(data: dict[str, Any]) -> Any:
    return _raise_if_failed(_write_market.do_m_payout(data))


def sync_update_m(data: dict[str, Any]) -> Any:
    return _raise_if_failed(_update_market.update_m(data))


def sync_user_account_message(data: dict[str, Any]) -> Any:
    """Apply user account payloads through the existing User backend functions."""
    action = data.get("action")
    if action == "USER_SIGNUP":
        return _raise_if_failed(_write_user.user_signup(data))
    if action == "USER_LOGIN":
        return _raise_if_failed(_write_user.user_login(data))
    if action == "USER_LOGOUT":
        return _raise_if_failed(_write_user.user_logout(data))
    if action == "UPDATE_USER_PROFILE":
        return _raise_if_failed(_update_user.update_user_profile(data))
    if action == "UPDATE_USER_PASSWORD":
        return _raise_if_failed(_update_user.update_user_password(data))
    if action == "TEST_PING":
        return True
    raise ValueError(f"unsupported user.account action: {action!r}")
