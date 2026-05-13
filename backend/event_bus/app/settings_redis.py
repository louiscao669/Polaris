"""Optional Redis for distributed read cache (ElastiCache, local Docker, etc.)."""

from __future__ import annotations

import os
from pathlib import Path


def _load_dotenv_files() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    app_dir = Path(__file__).resolve().parent
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


# When set, all read API nodes can share the same market/metadata cache.
# ElastiCache: use rediss:// for in-transit encryption (TLS).
REDIS_CACHE_URL: str = os.getenv("REDIS_CACHE_URL", "").strip()

# If true and Redis is unavailable at first use, fall back to in-process cache.
REDIS_CACHE_FALLBACK: bool = env_bool("REDIS_CACHE_FALLBACK", True)

# Key namespace to avoid collisions with other apps on a shared cluster.
REDIS_CACHE_KEY_PREFIX: str = os.getenv(
    "REDIS_CACHE_KEY_PREFIX", "polaris:event_bus:read:"
).strip() or "polaris:event_bus:read:"

# Long retention for "explicit" (no-TTL) keys; safety against unbounded growth.
REDIS_EXPLICIT_TTL_SECONDS: int = int(
    os.getenv("REDIS_EXPLICIT_TTL_SECONDS", "604800")
)  # 7d

# For read cache to be active, must have URL and not be disabled.
def redis_read_cache_configured() -> bool:
    if not REDIS_CACHE_URL:
        return False
    if env_bool("REDIS_CACHE_DISABLE", False):
        return False
    return True