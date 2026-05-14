"""Synchronous routing from Kafka payloads to mysql sync handlers."""

from __future__ import annotations

from typing import Any

from .kafka_handlers import (
    sync_create_e,
    sync_create_m,
    sync_create_o,
    sync_create_o_role,
    sync_create_o_token,
    sync_create_user_in_role,
    sync_delete_e,
    sync_delete_o,
    sync_designate_e_closed,
    sync_designate_e_contraint,
    sync_designate_e_market_creator,
    sync_designate_e_open_to,
    sync_designate_e_token,
    sync_designate_m_contraint,
    sync_designate_m_open_to_as,
    sync_designate_m_result,
    sync_designate_m_token,
    sync_do_m_payout,
    sync_do_m_transaction,
    sync_grant_o_token_to_user,
    sync_join_o,
    sync_leave_o,
    sync_remove_user_from_o,
    sync_update_e,
    sync_update_m,
    sync_update_o,
    sync_update_o_role,
    sync_update_o_token,
    sync_user_account_message,
)
from .kafka_producer import (
    TOPIC_ORGANIZATION,
    TOPIC_PLATFORM_EVENT,
    TOPIC_PLATFORM_MARKET,
    TOPIC_USER_IDENTITY,
)
from ..topics import (
    EVENT_LIFECYCLE,
    MARKET_OPERATIONS,
    ORG_MANAGEMENT,
    USER_ACCOUNT,
)


def dispatch_legacy_topic(topic: str, data: dict[str, Any]) -> None:
    """Original multi-topic layout (Docker / legacy publishers)."""
    action = data.get("action")

    if topic == TOPIC_ORGANIZATION:
        if action == "CREATE_ORGANIZATION":
            return sync_create_o(data)
        elif action == "CREATE_ORGANIZATION_ROLE":
            return sync_create_o_role(data)
        elif action == "CREATE_ORGANIZATION_TOKEN":
            return sync_create_o_token(data)
        elif action == "CREATE_ORGANIZATION_MEMBER":
            return sync_create_user_in_role(data)
        elif action == "JOIN_ORGANIZATION":
            return sync_join_o(data)
        elif action == "LEAVE_ORGANIZATION":
            return sync_leave_o(data)
        elif action == "REMOVE_ORGANIZATION_MEMBER":
            return sync_remove_user_from_o(data)
        elif action == "GRANT_ORGANIZATION_TOKEN":
            return sync_grant_o_token_to_user(data)
        elif action == "DELETE_ORGANIZATION":
            return sync_delete_o(data)
        elif action == "UPDATE_ORGANIZATION":
            return sync_update_o(data)
        elif action == "UPDATE_ORGANIZATION_ROLE":
            return sync_update_o_role(data)
        elif action == "UPDATE_ORGANIZATION_TOKEN":
            return sync_update_o_token(data)
        else:
            raise ValueError(f"unknown action for organization topic: {action!r}")

    elif topic == TOPIC_PLATFORM_EVENT:
        if action == "CREATE_EVENT":
            return sync_create_e(data)
        elif action == "DESIGNATE_EVENT_TOKEN":
            return sync_designate_e_token(data)
        elif action == "DESIGNATE_EVENT_MARKET_CREATOR":
            return sync_designate_e_market_creator(data)
        elif action == "DESIGNATE_EVENT_CONSTRAINT":
            return sync_designate_e_contraint(data)
        elif action == "DESIGNATE_EVENT_OPEN_TO":
            return sync_designate_e_open_to(data)
        elif action == "DESIGNATE_EVENT_CLOSED":
            return sync_designate_e_closed(data)
        elif action == "DELETE_EVENT":
            return sync_delete_e(data)
        elif action == "UPDATE_EVENT":
            return sync_update_e(data)
        else:
            raise ValueError(f"unknown action for platform.event topic: {action!r}")

    elif topic == TOPIC_PLATFORM_MARKET:
        if action == "CREATE_MARKET":
            return sync_create_m(data)
        elif action == "DESIGNATE_MARKET_TOKEN":
            return sync_designate_m_token(data)
        elif action == "DESIGNATE_MARKET_RESULT":
            return sync_designate_m_result(data)
        elif action == "DESIGNATE_MARKET_CONSTRAINT":
            return sync_designate_m_contraint(data)
        elif action == "DESIGNATE_MARKET_OPEN_TO_AS":
            return sync_designate_m_open_to_as(data)
        elif action == "MARKET_TRANSACTION":
            return sync_do_m_transaction(data)
        elif action == "MARKET_PAYOUT":
            return sync_do_m_payout(data)
        elif action == "UPDATE_MARKET":
            return sync_update_m(data)
        else:
            raise ValueError(f"unknown action for platform.market topic: {action!r}")

    elif topic == TOPIC_USER_IDENTITY:
        return sync_user_account_message(data)

    else:
        raise ValueError(f"unknown legacy topic: {topic!r}")


_MARKET_LIFECYCLE_ACTIONS = frozenset(
    {
        "CREATE_MARKET",
        "DESIGNATE_MARKET_TOKEN",
        "DESIGNATE_MARKET_RESULT",
        "DESIGNATE_MARKET_CONSTRAINT",
        "DESIGNATE_MARKET_OPEN_TO_AS",
        "UPDATE_MARKET",
    }
)
_MARKET_FINANCE_ACTIONS = frozenset({"MARKET_TRANSACTION", "MARKET_PAYOUT"})
_ORG_ACTIONS = frozenset(
    {
        "CREATE_ORGANIZATION",
        "CREATE_ORGANIZATION_ROLE",
        "CREATE_ORGANIZATION_TOKEN",
        "CREATE_ORGANIZATION_MEMBER",
        "JOIN_ORGANIZATION",
        "LEAVE_ORGANIZATION",
        "REMOVE_ORGANIZATION_MEMBER",
        "GRANT_ORGANIZATION_TOKEN",
        "DELETE_ORGANIZATION",
        "UPDATE_ORGANIZATION",
        "UPDATE_ORGANIZATION_ROLE",
        "UPDATE_ORGANIZATION_TOKEN",
    }
)
_EVENT_ACTIONS = frozenset(
    {
        "CREATE_EVENT",
        "DESIGNATE_EVENT_TOKEN",
        "DESIGNATE_EVENT_MARKET_CREATOR",
        "DESIGNATE_EVENT_CONSTRAINT",
        "DESIGNATE_EVENT_OPEN_TO",
        "DESIGNATE_EVENT_CLOSED",
        "DELETE_EVENT",
        "UPDATE_EVENT",
    }
)


def dispatch_v2_consolidated(consolidated_topic: str, payload: dict[str, Any]) -> Any:
    """v2 domain topics (MSK) map onto the same handlers as legacy topics."""
    action = payload.get("action")

    if consolidated_topic == ORG_MANAGEMENT:
        if action not in _ORG_ACTIONS:
            raise ValueError(f"unknown action for org.management topic: {action!r}")
        return dispatch_legacy_topic(TOPIC_ORGANIZATION, payload)

    if consolidated_topic == EVENT_LIFECYCLE:
        if action in _MARKET_LIFECYCLE_ACTIONS or action in _MARKET_FINANCE_ACTIONS:
            return dispatch_legacy_topic(TOPIC_PLATFORM_MARKET, payload)
        if action not in _EVENT_ACTIONS:
            raise ValueError(f"unknown action for event.lifecycle topic: {action!r}")
        return dispatch_legacy_topic(TOPIC_PLATFORM_EVENT, payload)

    if consolidated_topic == MARKET_OPERATIONS:
        if action in _MARKET_LIFECYCLE_ACTIONS:
            return dispatch_legacy_topic(TOPIC_PLATFORM_MARKET, payload)
        elif action in _MARKET_FINANCE_ACTIONS:
            return dispatch_legacy_topic(TOPIC_PLATFORM_MARKET, payload)
        else:
            raise ValueError(
                f"unknown action for market.operations topic: {action!r}"
            )

    if consolidated_topic == USER_ACCOUNT:
        return sync_user_account_message(payload)

    raise ValueError(f"unknown v2 consolidated topic: {consolidated_topic!r}")
