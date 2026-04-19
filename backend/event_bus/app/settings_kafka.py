"""Environment-driven Kafka client settings (local PLAINTEXT vs MSK IAM)."""

from __future__ import annotations

import os
from dotenv import load_dotenv
load_dotenv()

def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _bootstrap_servers_list() -> list[str]:
    raw = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092",
    )
    return [h.strip() for h in raw.split(",") if h.strip()]


KAFKA_BOOTSTRAP_SERVERS: list[str] = _bootstrap_servers_list()
KAFKA_USE_MSK_IAM: bool = env_bool("KAFKA_USE_MSK_IAM", False)
KAFKA_MSK_REGION: str = os.getenv("KAFKA_MSK_REGION", "us-east-2")

# Security protocol / mechanism
if KAFKA_USE_MSK_IAM:
    KAFKA_SECURITY_PROTOCOL = "SASL_SSL"
    KAFKA_SASL_MECHANISM = "OAUTHBEARER"
else:
    KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")
