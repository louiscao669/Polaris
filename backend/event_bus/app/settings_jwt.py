"""JWT verification settings for the async gateway."""

from __future__ import annotations

import os

from dotenv import load_dotenv
load_dotenv()

def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


V2_REQUIRE_JWT: bool = env_bool("V2_REQUIRE_JWT", False)
V2_OPERATIONS_REQUIRE_JWT: bool = env_bool("V2_OPERATIONS_REQUIRE_JWT", False)

JWT_ISSUER = os.getenv("JWT_ISSUER", "").strip() or None
JWT_AUDIENCE_RAW = os.getenv("JWT_AUDIENCE", "").strip()

JWT_PUBLIC_KEY_PATH = os.getenv("JWT_PUBLIC_KEY_PATH", "").strip() or None
JWT_PUBLIC_KEY_PEM = os.getenv("JWT_PUBLIC_KEY_PEM", "").strip() or None


def jwt_audiences() -> list[str] | None:
    if not JWT_AUDIENCE_RAW:
        return None
    return [a.strip() for a in JWT_AUDIENCE_RAW.split(",") if a.strip()]
