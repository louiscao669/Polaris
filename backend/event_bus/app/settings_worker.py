"""Worker process: consumer group + topic subscription per deployment."""

from __future__ import annotations

import os
from dotenv import load_dotenv
load_dotenv()

from .topics import (
    EVENT_LIFECYCLE,
    MARKET_OPERATIONS,
    ORG_MANAGEMENT,
    USER_ACCOUNT,
)

# Default consumer groups (match infra plan).
# ``market`` domain shares ``event.lifecycle`` with ``event`` — same default
# group so two domain-only fleets do not double-consume the same topic.
DOMAIN_DEFAULT_GROUP: dict[str, str] = {
    "market": "polaris-event-group",
    "event": "polaris-event-group",
    "org": "polaris-org-group",
    "user": "polaris-user-group",
}

DOMAIN_TOPICS: dict[str, list[str]] = {
    "market": [EVENT_LIFECYCLE],
    "event": [EVENT_LIFECYCLE],
    "org": [ORG_MANAGEMENT],
    "user": [USER_ACCOUNT],
}

# Includes ``market.operations`` for draining legacy/backlog messages only;
# new v2 HTTP publishes market commands on ``event.lifecycle``.
ALL_V2_TOPICS: list[str] = [
    MARKET_OPERATIONS,
    EVENT_LIFECYCLE,
    ORG_MANAGEMENT,
    USER_ACCOUNT,
]
DEFAULT_ALL_TOPICS_GROUP = "polaris-v2-worker"


def worker_topics_and_group() -> tuple[list[str], str]:
    """Return (topics, consumer_group_id).

    Market v2 commands (``CREATE_MARKET``, trades, etc.) are published on
    ``event.lifecycle``. Workers with ``POLARIS_WORKER_DOMAIN=event`` (or
    ``market``, same topic and default group) apply those handlers. Use
    ``POLARIS_WORKER_TOPICS`` or the default all-topics list to also subscribe
    to ``market.operations`` if you need to drain a legacy backlog.
    """
    raw_topics = os.getenv("POLARIS_WORKER_TOPICS", "").strip()
    group = os.getenv("POLARIS_WORKER_GROUP_ID", "").strip()
    domain = os.getenv("POLARIS_WORKER_DOMAIN", "").strip().lower()

    if raw_topics:
        topics = [t.strip() for t in raw_topics.split(",") if t.strip()]
    elif domain:
        topics = DOMAIN_TOPICS.get(domain, [])
        if not topics:
            raise ValueError(f"unknown POLARIS_WORKER_DOMAIN={domain!r}")
    else:
        topics = list(ALL_V2_TOPICS)

    if not group:
        if domain:
            group = DOMAIN_DEFAULT_GROUP.get(domain, "")
        if not group:
            group = DEFAULT_ALL_TOPICS_GROUP

    return topics, group
