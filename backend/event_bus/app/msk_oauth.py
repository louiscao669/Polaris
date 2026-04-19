"""MSK IAM (SASL/OAUTHBEARER) token helpers for aiokafka.

Tutorial-style synchronous token::

    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

    def get_msk_auth_token(region: str) -> str:
        token, _ = MSKAuthTokenProvider.generate_auth_token(region)
        return token

**aiokafka** does not accept a bare function for ``sasl_oauth_token_provider`` — it must
implement :class:`aiokafka.abc.AbstractTokenProvider` with ``async def token()``.
:class:`MskIamTokenProvider` wraps :func:`get_msk_auth_token` on a thread pool.

Also, **always** use ``sasl_mechanism="OAUTHBEARER"`` with aiokafka for MSK IAM.
Some docs show ``AWS_MSK_IAM``; that is **not** a valid aiokafka mechanism name — the broker
still negotiates **OAUTHBEARER** for IAM.
"""

from __future__ import annotations

import asyncio
import ssl
from typing import Optional

from aiokafka.abc import AbstractTokenProvider

try:
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
except ImportError:  # pragma: no cover - optional dependency at install time
    MSKAuthTokenProvider = None  # type: ignore[misc, assignment]


def get_msk_auth_token(region: str) -> str:
    """Sync token fetch (same body as common MSK IAM examples)."""
    if MSKAuthTokenProvider is None:
        raise RuntimeError(
            "MSK IAM requested but aws-msk-iam-sasl-signer-python is not installed"
        )
    token, _expiry_ms = MSKAuthTokenProvider.generate_auth_token(region)
    return token


def default_ssl_context() -> ssl.SSLContext:
    return ssl.create_default_context()


class MskIamTokenProvider(AbstractTokenProvider):
    """aiokafka calls ``token()`` for each auth attempt; MSK signer is sync so we offload."""

    def __init__(self, region: str) -> None:
        self._region = region

    async def token(self) -> Optional[str]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            get_msk_auth_token,
            self._region,
        )
