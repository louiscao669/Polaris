"""Canonical Kafka topic names for Polaris v2 (MSK + IAM)."""

MARKET_OPERATIONS = "market.operations"
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


def dlq_for(topic: str) -> str | None:
    return V2_DLQ_BY_TOPIC.get(topic)
