"""Synchronous routing from Kafka payloads to mysql sync handlers."""

from __future__ import annotations

from typing import Any

from .kafka_consumer_sync import (
    sync_create_e,
    sync_create_m,
    sync_create_o,
    sync_create_o_role,
    sync_create_o_token,
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
    sync_user_account_message,
)
from .kafka_producer import (
    TOPIC_ORGANIZATION,
    TOPIC_PLATFORM_EVENT,
    TOPIC_PLATFORM_MARKET,
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
            sync_create_o(data)
        elif action == "CREATE_ORGANIZATION_ROLE":
            sync_create_o_role(data)
        elif action == "CREATE_ORGANIZATION_TOKEN":
            sync_create_o_token(data)
        else:
            raise ValueError(f"unknown action for organization topic: {action!r}")

    elif topic == TOPIC_PLATFORM_EVENT:
        if action == "CREATE_EVENT":
            sync_create_e(data)
        elif action == "DESIGNATE_EVENT_TOKEN":
            sync_designate_e_token(data)
        elif action == "DESIGNATE_EVENT_MARKET_CREATOR":
            sync_designate_e_market_creator(data)
        elif action == "DESIGNATE_EVENT_CONSTRAINT":
            sync_designate_e_contraint(data)
        elif action == "DESIGNATE_EVENT_OPEN_TO":
            sync_designate_e_open_to(data)
        elif action == "DESIGNATE_EVENT_CLOSED":
            sync_designate_e_closed(data)
        else:
            raise ValueError(f"unknown action for platform.event topic: {action!r}")

    elif topic == TOPIC_PLATFORM_MARKET:
        if action == "CREATE_MARKET":
            sync_create_m(data)
        elif action == "DESIGNATE_MARKET_TOKEN":
            sync_designate_m_token(data)
        elif action == "DESIGNATE_MARKET_RESULT":
            sync_designate_m_result(data)
        elif action == "DESIGNATE_MARKET_CONSTRAINT":
            sync_designate_m_contraint(data)
        elif action == "DESIGNATE_MARKET_OPEN_TO_AS":
            sync_designate_m_open_to_as(data)
        elif action == "MARKET_TRANSACTION":
            sync_do_m_transaction(data)
        elif action == "MARKET_PAYOUT":
            sync_do_m_payout(data)
        else:
            raise ValueError(f"unknown action for platform.market topic: {action!r}")

    else:
        raise ValueError(f"unknown legacy topic: {topic!r}")


_MARKET_LIFECYCLE_ACTIONS = frozenset(
    {
        "CREATE_MARKET",
        "DESIGNATE_MARKET_TOKEN",
        "DESIGNATE_MARKET_RESULT",
        "DESIGNATE_MARKET_CONSTRAINT",
        "DESIGNATE_MARKET_OPEN_TO_AS",
    }
)
_MARKET_FINANCE_ACTIONS = frozenset({"MARKET_TRANSACTION", "MARKET_PAYOUT"})


def dispatch_v2_consolidated(consolidated_topic: str, payload: dict[str, Any]) -> None:
    """v2 domain topics (MSK) map onto the same handlers as legacy topics."""
    action = payload.get("action")

    if consolidated_topic == ORG_MANAGEMENT:
        dispatch_legacy_topic(TOPIC_ORGANIZATION, payload)
        return

    if consolidated_topic == EVENT_LIFECYCLE:
        dispatch_legacy_topic(TOPIC_PLATFORM_EVENT, payload)
        return

    if consolidated_topic == MARKET_OPERATIONS:
        if action in _MARKET_LIFECYCLE_ACTIONS:
            dispatch_legacy_topic(TOPIC_PLATFORM_MARKET, payload)
        elif action in _MARKET_FINANCE_ACTIONS:
            dispatch_legacy_topic(TOPIC_PLATFORM_MARKET, payload)
        else:
            raise ValueError(
                f"unknown action for market.operations topic: {action!r}"
            )
        return

    if consolidated_topic == USER_ACCOUNT:
        sync_user_account_message(payload)
        return

    raise ValueError(f"unknown v2 consolidated topic: {consolidated_topic!r}")
