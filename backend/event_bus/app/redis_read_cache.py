"""Redis-backed read caches (shared across API instances). Falls back to in-process memory."""

from __future__ import annotations

import json
from typing import Any, Type

from .read_cache_memory import (
    ExplicitCache,
    TTLCache,
    cache_enabled_for_request,
)
from .settings_redis import (
    REDIS_CACHE_KEY_PREFIX,
    REDIS_EXPLICIT_TTL_SECONDS,
    REDIS_CACHE_FALLBACK,
    redis_read_cache_configured,
)

try:
    import redis
    from redis.exceptions import RedisError
except ImportError:  # pragma: no cover
    redis = None  # type: ignore[assignment]
    RedisError = Exception  # type: ignore[misc, assignment]

_client: Any = None
_client_failed: bool = False


def _json_dumps(value: Any) -> str:
    return json.dumps(value, default=str, separators=(",", ":"))


def _json_loads(raw: str) -> Any:
    return json.loads(raw)


def _get_client() -> Any:
    global _client, _client_failed
    if redis is None or _client_failed:
        return None
    if _client is not None:
        return _client
    if not redis_read_cache_configured():
        return None

    from .settings_redis import REDIS_CACHE_URL

    try:
        c = redis.Redis.from_url(  # type: ignore[union-attr]
            REDIS_CACHE_URL,
            decode_responses=True,
            socket_connect_timeout=2.5,
            socket_timeout=5.0,
            health_check_interval=30,
        )
        c.ping()
    except Exception:
        if not REDIS_CACHE_FALLBACK:
            raise
        _client_failed = True
        print(
            "WARNING: REDIS_CACHE_URL set but Redis ping failed — "
            "using in-process read cache only on this node.",
            flush=True,
        )
        return None
    _client = c
    print("Redis read cache: connected (shared across API instances).", flush=True)
    return _client


class RedisTTLCache:
    """Same contract as TTLCache; uses Redis with in-process fallback."""

    def __init__(self, *, key_prefix: str, mem: TTLCache) -> None:
        self._key_prefix = key_prefix
        self._mem = mem

    def _full(self, key: str) -> str:
        return f"{self._key_prefix}{key}"

    def get(self, key: str) -> Any | None:
        if not cache_enabled_for_request():
            return None
        r = _get_client()
        if r is None:
            return self._mem.get(key)
        try:
            raw = r.get(self._full(key))
            if raw is None:
                return None
            return _json_loads(raw)
        except RedisError:
            return self._mem.get(key)

    def set(self, key: str, value: Any, ttl_seconds: float) -> None:
        if not cache_enabled_for_request():
            return
        r = _get_client()
        if r is None:
            self._mem.set(key, value, ttl_seconds)
            return
        ttl = max(1, int(ttl_seconds))
        try:
            r.set(self._full(key), _json_dumps(value), ex=ttl)
        except RedisError:
            self._mem.set(key, value, ttl_seconds)

    def delete_prefix(self, prefix: str) -> int:
        n_mem = self._mem.delete_prefix(prefix)
        r = _get_client()
        if r is None:
            return n_mem
        pattern = f"{self._key_prefix}{prefix}*"
        deleted = 0
        try:
            for k in r.scan_iter(match=pattern, count=500):
                r.delete(k)
                deleted += 1
            return deleted + n_mem
        except RedisError:
            return n_mem


class RedisExplicitCache:
    """Same contract as ExplicitCache; Redis + long safety TTL + memory fallback."""

    def __init__(
        self, *, key_prefix: str, mem: ExplicitCache, explicit_ttl_seconds: int
    ) -> None:
        self._key_prefix = key_prefix
        self._mem = mem
        self._explicit_ttl = max(60, int(explicit_ttl_seconds))

    def _full(self, key: str) -> str:
        return f"{self._key_prefix}{key}"

    def get(self, key: str) -> Any | None:
        if not cache_enabled_for_request():
            return None
        r = _get_client()
        if r is None:
            return self._mem.get(key)
        try:
            raw = r.get(self._full(key))
            if raw is None:
                return None
            return _json_loads(raw)
        except RedisError:
            return self._mem.get(key)

    def set(self, key: str, value: Any) -> None:
        if not cache_enabled_for_request():
            return
        r = _get_client()
        if r is None:
            self._mem.set(key, value)
            return
        try:
            r.set(self._full(key), _json_dumps(value), ex=self._explicit_ttl)
        except RedisError:
            self._mem.set(key, value)

    def delete_prefix(self, prefix: str) -> int:
        n_mem = self._mem.delete_prefix(prefix)
        r = _get_client()
        if r is None:
            return n_mem
        pattern = f"{self._key_prefix}{prefix}*"
        deleted = 0
        try:
            for k in r.scan_iter(match=pattern, count=500):
                r.delete(k)
                deleted += 1
            return deleted + n_mem
        except RedisError:
            return n_mem

    def delete_matching_fragment(self, fragment: str) -> int:
        n_mem = self._mem.delete_matching_fragment(fragment)
        r = _get_client()
        if r is None:
            return n_mem
        deleted = 0
        try:
            for k in r.scan_iter(match=f"{self._key_prefix}*", count=500):
                if fragment in k:
                    r.delete(k)
                    deleted += 1
            return deleted + n_mem
        except RedisError:
            return n_mem


def make_read_caches(
    *,
    ttl_type: Type[TTLCache] = TTLCache,
    explicit_type: Type[ExplicitCache] = ExplicitCache,
) -> tuple[Any, Any]:
    mem_ttl = ttl_type()
    mem_exp = explicit_type()
    if not redis_read_cache_configured() or redis is None:
        return mem_ttl, mem_exp

    prefix = REDIS_CACHE_KEY_PREFIX
    if not prefix.endswith(":"):
        prefix = prefix + ":"
    return (
        RedisTTLCache(key_prefix=prefix, mem=mem_ttl),
        RedisExplicitCache(
            key_prefix=prefix,
            mem=mem_exp,
            explicit_ttl_seconds=REDIS_EXPLICIT_TTL_SECONDS,
        ),
    )
