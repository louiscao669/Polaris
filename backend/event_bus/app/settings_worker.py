"""Worker process: consumer group + topic subscription per deployment."""

from __future__ import annotations

import os
from dotenv import load_dotenv
load_dotenv()

from backend.app.topics import (
    EVENT_LIFECYCLE,
    MARKET_OPERATIONS,
    ORG_MANAGEMENT,
    USER_ACCOUNT,
)

# Default consumer groups (match infra plan)
DOMAIN_DEFAULT_GROUP: dict[str, str] = {
    "market": "polaris-market-group",
    "event": "polaris-event-group",
    "org": "polaris-org-group",
    "user": "polaris-user-group",
}

DOMAIN_TOPICS: dict[str, list[str]] = {
    "market": [MARKET_OPERATIONS],
    "event": [EVENT_LIFECYCLE],
    "org": [ORG_MANAGEMENT],
    "user": [USER_ACCOUNT],
}


def worker_topics_and_group() -> tuple[list[str], str]:
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
        raise ValueError(
            "Set POLARIS_WORKER_TOPICS or POLARIS_WORKER_DOMAIN for v2 worker mode"
        )

    if not group:
        if domain:
            group = DOMAIN_DEFAULT_GROUP.get(domain, "")
        if not group:
            raise ValueError(
                "Set POLARIS_WORKER_GROUP_ID or use a known POLARIS_WORKER_DOMAIN"
            )

    return topics, group
