"""Read-heavy endpoint caching: in-process by default; optional Redis for multi-instance."""

from __future__ import annotations

from .read_cache_memory import (
    ExplicitCache,
    TTLCache,
    cache_enabled_for_request,
    cache_mode,
)
from .redis_read_cache import make_read_caches

market_read_cache, metadata_read_cache = make_read_caches()


def market_stats_key(
    *,
    market_id: int,
    user_id: int,
    stat_name: str,
    extra: str | None = None,
) -> str:
    suffix = f":{extra}" if extra else ""
    return f"market:{market_id}:user:{user_id}:stats:{stat_name}{suffix}"


def invalidate_market_stats_cache(market_id: int) -> int:
    return market_read_cache.delete_prefix(f"market:{market_id}:")


def market_detail_key(*, market_id: int, user_id: int) -> str:
    return f"market:{market_id}:user:{user_id}:detail"


def event_markets_key(*, event_id: int, user_id: int) -> str:
    return f"event:{event_id}:user:{user_id}:markets"


def org_events_key(*, organization_id: int, user_id: int) -> str:
    return f"org:{organization_id}:user:{user_id}:events"


def invalidate_market_detail_cache(market_id: int) -> int:
    return metadata_read_cache.delete_prefix(f"market:{market_id}:")


def invalidate_event_markets_cache(event_id: int) -> int:
    return metadata_read_cache.delete_prefix(f"event:{event_id}:")


def invalidate_org_events_cache(organization_id: int) -> int:
    return metadata_read_cache.delete_prefix(f"org:{organization_id}:")


def invalidate_user_metadata_cache(user_id: int) -> int:
    return metadata_read_cache.delete_matching_fragment(f":user:{user_id}:")
