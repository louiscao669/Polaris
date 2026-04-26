"""JWT minting and verification helpers for Polaris access tokens."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

import jwt

from ..settings_jwt import (
    JWT_ACCESS_TTL_SECONDS,
    JWT_AUDIENCE_RAW,
    JWT_ISSUER,
    JWT_PRIVATE_KEY_PATH,
    JWT_PRIVATE_KEY_PEM,
    JWT_PUBLIC_KEY_PATH,
    JWT_PUBLIC_KEY_PEM,
    JWT_SECRET,
    JWT_SIGNING_ALG,
    jwt_audiences,
)


class JWTConfigurationError(RuntimeError):
    pass


def _normalize_pem(raw: str | None) -> str | None:
    if not raw:
        return None
    return raw.replace("\\n", "\n").strip()


def _read_key_from_path(path_value: str | None) -> str | None:
    if not path_value:
        return None
    path = Path(path_value)
    if not path.is_file():
        raise JWTConfigurationError(f"JWT key path not found: {path}")
    return path.read_text(encoding="utf-8").strip()


@lru_cache(maxsize=1)
def signing_key() -> str:
    if JWT_SIGNING_ALG.startswith("HS"):
        if JWT_SECRET:
            return JWT_SECRET
        raise JWTConfigurationError("HS JWT signing requires JWT_SECRET")

    pem = _normalize_pem(JWT_PRIVATE_KEY_PEM) or _read_key_from_path(JWT_PRIVATE_KEY_PATH)
    if pem:
        return pem
    raise JWTConfigurationError(
        "RS JWT signing requires JWT_PRIVATE_KEY_PEM or JWT_PRIVATE_KEY_PATH"
    )


@lru_cache(maxsize=1)
def verification_key() -> str:
    if JWT_SIGNING_ALG.startswith("HS"):
        if JWT_SECRET:
            return JWT_SECRET
        raise JWTConfigurationError("HS JWT verification requires JWT_SECRET")

    pem = _normalize_pem(JWT_PUBLIC_KEY_PEM) or _read_key_from_path(JWT_PUBLIC_KEY_PATH)
    if pem:
        return pem
    raise JWTConfigurationError(
        "RS JWT verification requires JWT_PUBLIC_KEY_PEM or JWT_PUBLIC_KEY_PATH"
    )


def mint_access_token(
    *,
    user_id: int,
    username: str,
    first: str | None = None,
    last: str | None = None,
    org_id: int | None = None,
    scopes: list[str] | None = None,
    ttl_seconds: int = JWT_ACCESS_TTL_SECONDS,
) -> tuple[str, datetime]:
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(seconds=ttl_seconds)

    payload: dict[str, Any] = {
        "sub": str(user_id),
        "user_id": user_id,
        "username": username,
        "iat": int(now.timestamp()),
        "exp": int(expires_at.timestamp()),
    }

    if first:
        payload["first"] = first
    if last:
        payload["last"] = last
    if org_id is not None:
        payload["org_id"] = org_id
    if scopes:
        payload["scope"] = " ".join(scopes)
    if JWT_ISSUER:
        payload["iss"] = JWT_ISSUER

    audiences = jwt_audiences()
    if audiences:
        payload["aud"] = audiences[0] if len(audiences) == 1 else audiences

    token = jwt.encode(payload, signing_key(), algorithm=JWT_SIGNING_ALG)
    return token, expires_at


def decode_access_token(token: str) -> dict[str, Any]:
    options = {"require": ["exp", "sub"]}
    decode_kwargs: dict[str, Any] = {
        "algorithms": [JWT_SIGNING_ALG],
        "options": options,
    }

    if JWT_ISSUER:
        decode_kwargs["issuer"] = JWT_ISSUER

    audiences = jwt_audiences()
    if audiences:
        decode_kwargs["audience"] = audiences[0] if len(audiences) == 1 else audiences
    elif not JWT_AUDIENCE_RAW:
        decode_kwargs["options"] = {**options, "verify_aud": False}

    return jwt.decode(token, verification_key(), **decode_kwargs)
