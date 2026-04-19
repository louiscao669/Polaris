"""Environment-driven Kafka client settings (local PLAINTEXT vs MSK IAM).

Loads ``.env`` from the repo root and from ``backend/event_bus/.env`` before reading
variables so ``KAFKA_BOOTSTRAP_SERVERS`` is picked up regardless of process cwd.
"""

from __future__ import annotations

import os
from pathlib import Path


def _load_dotenv_files() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    app_dir = Path(__file__).resolve().parent  # .../event_bus/app
    # …/backend/event_bus/app → parents[2] = repo root, parents[0] = …/event_bus
    root_env = app_dir.parents[2] / ".env"
    event_bus_env = app_dir.parents[0] / ".env"
    if root_env.is_file():
        load_dotenv(root_env)
    if event_bus_env.is_file():
        load_dotenv(event_bus_env, override=True)


_load_dotenv_files()


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _bootstrap_servers_list() -> list[str]:
    raw = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "").strip()
    if not raw:
        raw = "localhost:9092"
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
