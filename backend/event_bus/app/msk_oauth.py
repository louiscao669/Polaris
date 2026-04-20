"""MSK IAM (SASL/OAUTHBEARER) helpers for aiokafka + Amazon MSK.

Tutorial pattern you may see::

    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

    def oauth_cb(args):  # or oauth_cb()
        token, expiry = MSKAuthTokenProvider.generate_auth_token(\"us-east-2\")
        return token

    producer = AIOKafkaProducer(
        bootstrap_servers=\"broker:9098\",
        security_protocol=\"SASL_SSL\",
        sasl_mechanism=\"OAUTHBEARER\",
        sasl_oauth_token_provider=oauth_cb,  # <-- NOT valid in aiokafka
    )

**aiokafka** validates ``sasl_oauth_token_provider`` with
``isinstance(..., AbstractTokenProvider)``. A plain function like ``oauth_cb`` will
raise at connection time. Use :class:`MskIamTokenProvider` instead — it runs the same
``MSKAuthTokenProvider.generate_auth_token(region)`` call (sync signer uses the EC2
instance profile / standard AWS credential chain).

Same wire protocol as your snippet: ``SASL_SSL`` + ``OAUTHBEARER`` on port **9098**.
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


def msk_oauth_sync(region: str) -> str:
    """Same logic as a tutorial ``oauth_cb`` body: IAM token string for MSK.

    ``MSKAuthTokenProvider.generate_auth_token`` uses default AWS credentials
    (instance/task role on EC2/ECS, env, profile, etc.).
    """
    if MSKAuthTokenProvider is None:
        raise RuntimeError(
            "MSK IAM requested but aws-msk-iam-sasl-signer-python is not installed"
        )
    token, _expiry_ms = MSKAuthTokenProvider.generate_auth_token(region)
    return token


def default_ssl_context() -> ssl.SSLContext:
    return ssl.create_default_context()


class MskIamTokenProvider(AbstractTokenProvider):
    """Wraps :func:`msk_oauth_sync` so aiokafka can call ``await provider.token()``."""

    def __init__(self, region: str) -> None:
        self._region = region

    async def token(self) -> Optional[str]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, msk_oauth_sync, self._region)
