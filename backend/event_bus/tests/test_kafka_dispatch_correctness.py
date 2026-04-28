"""Routing correctness: v2 consolidated topics map to the same handlers as legacy."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from app.core.kafka_dispatch import dispatch_v2_consolidated
from app.topics import EVENT_LIFECYCLE, MARKET_OPERATIONS, ORG_MANAGEMENT, USER_ACCOUNT


def test_market_operations_market_transaction_calls_sync_handler() -> None:
    payload = {
        "action": "MARKET_TRANSACTION",
        "user_id": 1,
        "market_id": 2,
        "transaction_id": 999,
    }
    with patch("app.core.kafka_dispatch.sync_do_m_transaction") as fn:
        dispatch_v2_consolidated(MARKET_OPERATIONS, payload)
        fn.assert_called_once_with(payload)


def test_market_operations_unknown_action_raises() -> None:
    with pytest.raises(ValueError, match="unknown action"):
        dispatch_v2_consolidated(MARKET_OPERATIONS, {"action": "UNKNOWN_ACTION_X"})


def test_event_lifecycle_delegates_to_event_handlers() -> None:
    payload = {"action": "CREATE_EVENT", "user_id": 1, "event_id": 1}
    with patch("app.core.kafka_dispatch.sync_create_e") as fn:
        dispatch_v2_consolidated(EVENT_LIFECYCLE, payload)
        fn.assert_called_once_with(payload)


def test_org_management_delegates_to_org_handlers() -> None:
    payload = {"action": "CREATE_ORGANIZATION", "user_id": 1}
    with patch("app.core.kafka_dispatch.sync_create_o") as fn:
        dispatch_v2_consolidated(ORG_MANAGEMENT, payload)
        fn.assert_called_once_with(payload)


def test_user_account_calls_signup_handler() -> None:
    payload = {"action": "USER_SIGNUP", "email": "a@example.com"}
    with patch("app.core.kafka_dispatch.sync_user_account_message") as fn:
        dispatch_v2_consolidated(USER_ACCOUNT, payload)
        fn.assert_called_once_with(payload)


def test_unknown_consolidated_topic_raises() -> None:
    with pytest.raises(ValueError, match="unknown v2 consolidated"):
        dispatch_v2_consolidated("not.a.real.topic", {})
