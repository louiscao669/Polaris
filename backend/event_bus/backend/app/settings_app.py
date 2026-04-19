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
POLARIS_ENABLE_V2_WORKER: bool = env_bool("POLARIS_ENABLE_V2_WORKER", False)
