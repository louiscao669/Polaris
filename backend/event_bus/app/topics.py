"""Canonical Kafka topic names for Polaris v2 (MSK + IAM)."""
from typing import Optional
MARKET_OPERATIONS = "market.operations"  # Legacy/backlog; v2 HTTP publishes market commands on EVENT_LIFECYCLE.
MARKET_OPERATIONS_DLQ = "market.operations.dlq"

EVENT_LIFECYCLE = "event.lifecycle"
EVENT_LIFECYCLE_DLQ = "event.lifecycle.dlq"

ORG_MANAGEMENT = "org.management"
ORG_MANAGEMENT_DLQ = "org.management.dlq"

USER_ACCOUNT = "user.account"
USER_ACCOUNT_DLQ = "user.account.dlq"

V2_DLQ_BY_TOPIC: dict[str, str] = {
    MARKET_OPERATIONS: MARKET_OPERATIONS_DLQ,
    EVENT_LIFECYCLE: EVENT_LIFECYCLE_DLQ,
    ORG_MANAGEMENT: ORG_MANAGEMENT_DLQ,
    USER_ACCOUNT: USER_ACCOUNT_DLQ,
}


def dlq_for(topic: str) -> Optional[str]:
    return V2_DLQ_BY_TOPIC.get(topic)
