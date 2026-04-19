"""FastAPI dependencies: Bearer JWT → claims dict."""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from backend.app.auth.jwt_verify import JWTConfigurationError, decode_access_token
from backend.app.settings_jwt import V2_OPERATIONS_REQUIRE_JWT, V2_REQUIRE_JWT

security = HTTPBearer(auto_error=False)


async def bearer_claims_optional(
    credentials: Annotated[
        HTTPAuthorizationCredentials | None, Depends(security)
    ],
) -> dict[str, Any] | None:
    if credentials is None or credentials.scheme.lower() != "bearer":
        return None
    try:
        return decode_access_token(credentials.credentials)
    except JWTConfigurationError as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(e),
        ) from e
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid token",
        ) from e


async def bearer_claims_required_for_writes(
    credentials: Annotated[
        HTTPAuthorizationCredentials | None, Depends(security)
    ],
) -> dict[str, Any]:
    if not V2_REQUIRE_JWT:
        if credentials is None or credentials.scheme.lower() != "bearer":
            return {}
        try:
            return decode_access_token(credentials.credentials)
        except JWTConfigurationError as e:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=str(e),
            ) from e
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="invalid token",
            ) from e

    if credentials is None or credentials.scheme.lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing bearer token",
        )
    try:
        return decode_access_token(credentials.credentials)
    except JWTConfigurationError as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(e),
        ) from e
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid token",
        ) from e


async def bearer_claims_required_for_operations_read(
    credentials: Annotated[
        HTTPAuthorizationCredentials | None, Depends(security)
    ],
) -> dict[str, Any]:
    if not V2_OPERATIONS_REQUIRE_JWT:
        return {}

    if credentials is None or credentials.scheme.lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing bearer token",
        )
    try:
        return decode_access_token(credentials.credentials)
    except JWTConfigurationError as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(e),
        ) from e
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid token",
        ) from e
