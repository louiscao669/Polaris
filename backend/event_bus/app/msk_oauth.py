"""MSK IAM (SASL/OAUTHBEARER) token provider for aiokafka."""

from __future__ import annotations

import asyncio
import ssl
from typing import Optional

from aiokafka.abc import AbstractTokenProvider

try:
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
except ImportError:  # pragma: no cover - optional dependency at install time
    MSKAuthTokenProvider = None  # type: ignore[misc, assignment]


def default_ssl_context() -> ssl.SSLContext:
    return ssl.create_default_context()


class MskIamTokenProvider(AbstractTokenProvider):
    """Async token provider; runs the blocking MSK signer in a thread."""

    def __init__(self, region: str) -> None:
        self._region = region

    async def token(self) -> Optional[str]:
        if MSKAuthTokenProvider is None:
            raise RuntimeError(
                "MSK IAM requested but aws-msk-iam-sasl-signer-python is not installed"
            )
        loop = asyncio.get_running_loop()

        def _sync_token() -> str:
            token, _expiry_ms = MSKAuthTokenProvider.generate_auth_token(self._region)
            return token

        return await loop.run_in_executor(None, _sync_token)
