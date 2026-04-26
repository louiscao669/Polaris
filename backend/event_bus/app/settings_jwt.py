"""JWT settings for access-token minting and verification."""

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
JWT_SIGNING_ALG = (os.getenv("JWT_SIGNING_ALG", "HS256").strip() or "HS256").upper()

JWT_SECRET = os.getenv("JWT_SECRET", "").strip() or "polaris-dev-secret-change-me"

JWT_PUBLIC_KEY_PATH = os.getenv("JWT_PUBLIC_KEY_PATH", "").strip() or None
JWT_PUBLIC_KEY_PEM = os.getenv("JWT_PUBLIC_KEY_PEM", "").strip() or None
JWT_PRIVATE_KEY_PATH = os.getenv("JWT_PRIVATE_KEY_PATH", "").strip() or None
JWT_PRIVATE_KEY_PEM = os.getenv("JWT_PRIVATE_KEY_PEM", "").strip() or None

JWT_ACCESS_TTL_SECONDS = int(os.getenv("JWT_ACCESS_TTL_SECONDS", "604800"))


def jwt_audiences() -> list[str] | None:
    if not JWT_AUDIENCE_RAW:
        return None
    return [a.strip() for a in JWT_AUDIENCE_RAW.split(",") if a.strip()]
