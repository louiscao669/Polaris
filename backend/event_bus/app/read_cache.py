"""Small in-process TTL cache for read-heavy endpoints."""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator
from typing import Any


@dataclass
class CacheEntry:
    value: Any
    expires_at: float


_cache_request_state = threading.local()


def _cache_mode() -> str:
    return getattr(_cache_request_state, "mode", "default")


def cache_enabled_for_request() -> bool:
    return _cache_mode() != "bypass"


@contextmanager
def cache_mode(mode: str) -> Iterator[None]:
    previous = _cache_mode()
    _cache_request_state.mode = mode
    try:
        yield
    finally:
        _cache_request_state.mode = previous


class TTLCache:
    def __init__(self) -> None:
        self._entries: dict[str, CacheEntry] = {}
        self._lock = threading.RLock()

    def get(self, key: str) -> Any | None:
        if not cache_enabled_for_request():
            return None
        now = time.time()
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            if entry.expires_at <= now:
                self._entries.pop(key, None)
                return None
            return entry.value

    def set(self, key: str, value: Any, ttl_seconds: float) -> None:
        if not cache_enabled_for_request():
            return
        expires_at = time.time() + ttl_seconds
        with self._lock:
            self._entries[key] = CacheEntry(value=value, expires_at=expires_at)

    def delete_prefix(self, prefix: str) -> int:
        with self._lock:
            doomed = [key for key in self._entries if key.startswith(prefix)]
            for key in doomed:
                self._entries.pop(key, None)
            return len(doomed)


class ExplicitCache:
    def __init__(self) -> None:
        self._entries: dict[str, Any] = {}
        self._lock = threading.RLock()

    def get(self, key: str) -> Any | None:
        if not cache_enabled_for_request():
            return None
        with self._lock:
            return self._entries.get(key)

    def set(self, key: str, value: Any) -> None:
        if not cache_enabled_for_request():
            return
        with self._lock:
            self._entries[key] = value

    def delete_prefix(self, prefix: str) -> int:
        with self._lock:
            doomed = [key for key in self._entries if key.startswith(prefix)]
            for key in doomed:
                self._entries.pop(key, None)
            return len(doomed)

    def delete_matching_fragment(self, fragment: str) -> int:
        with self._lock:
            doomed = [key for key in self._entries if fragment in key]
            for key in doomed:
                self._entries.pop(key, None)
            return len(doomed)


market_read_cache = TTLCache()
metadata_read_cache = ExplicitCache()


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
