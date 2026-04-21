"""Process role toggles (gateway HTTP vs embedded consumers vs v2 worker)."""

from __future__ import annotations

import os

from dotenv import load_dotenv

load_dotenv()


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


POLARIS_ENABLE_LEGACY_CONSUMER: bool = env_bool("POLARIS_ENABLE_LEGACY_CONSUMER", True)
# Default on so /v2 HTTP writes have a consumer in the common single-process setup.
POLARIS_ENABLE_V2_WORKER: bool = env_bool("POLARIS_ENABLE_V2_WORKER", True)

# When true: do not connect Kafka during FastAPI lifespan (health/API up; Kafka writes fail).
POLARIS_SKIP_KAFKA_AT_STARTUP: bool = env_bool("POLARIS_SKIP_KAFKA_AT_STARTUP", False)

# When true: lifespan **raises** if Kafka bootstrap fails (strict).
# Default false: log the error and continue so HTTP /health works while you fix MSK SG/VPC.
POLARIS_REQUIRE_KAFKA_AT_STARTUP: bool = env_bool(
    "POLARIS_REQUIRE_KAFKA_AT_STARTUP", False
)
