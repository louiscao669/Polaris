"""RS256 JWT verification for gateway routes (public key from env / file)."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import jwt

from ..settings_jwt import (
    JWT_ISSUER,
    JWT_PUBLIC_KEY_PATH,
    JWT_PUBLIC_KEY_PEM,
    jwt_audiences,
)


class JWTConfigurationError(RuntimeError):
    pass


@lru_cache(maxsize=1)
def _public_key_pem_from_env() -> str:
    if JWT_PUBLIC_KEY_PEM:
        return JWT_PUBLIC_KEY_PEM.replace("\\n", "\n").strip()
    if JWT_PUBLIC_KEY_PATH:
        p = Path(JWT_PUBLIC_KEY_PATH)
        if not p.is_file():
            raise JWTConfigurationError(f"JWT_PUBLIC_KEY_PATH not found: {p}")
        return p.read_text(encoding="utf-8").strip()
    raise JWTConfigurationError(
        "JWT verification requires JWT_PUBLIC_KEY_PEM or JWT_PUBLIC_KEY_PATH"
    )


def decode_access_token(token: str) -> dict[str, Any]:
    """Verify RS256 JWT; raises jwt.PyJWTError on failure."""
    key = _public_key_pem_from_env()
    audiences = jwt_audiences()
    options = {"require": ["exp", "sub"]}

    decode_kwargs: dict[str, Any] = {
        "algorithms": ["RS256"],
        "options": options,
    }
    if JWT_ISSUER:
        decode_kwargs["issuer"] = JWT_ISSUER

    aud_list = audiences
    if aud_list:
        decode_kwargs["audience"] = aud_list[0] if len(aud_list) == 1 else aud_list

    return jwt.decode(token, key, **decode_kwargs)
