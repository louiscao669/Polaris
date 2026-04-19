"""Shared aiokafka consumer/producer SSL + MSK IAM kwargs.

Equivalent to::

    token, _ = MSKAuthTokenProvider.generate_auth_token(region)  # see get_msk_auth_token

    AIOKafkaProducer(
        bootstrap_servers=[...],
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        sasl_oauth_token_provider=MskIamTokenProvider(region),
        ssl_context=ssl.create_default_context(),
    )

``sasl_mechanism`` must be **OAUTHBEARER** for aiokafka + MSK IAM — not ``AWS_MSK_IAM``.
"""

from __future__ import annotations

import ssl
from typing import Any

from .msk_oauth import MskIamTokenProvider
from .settings_kafka import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_MSK_REGION,
    KAFKA_SECURITY_PROTOCOL,
    KAFKA_USE_MSK_IAM,
)


def _msk_iam_kwargs() -> dict[str, Any]:
    """Explicit MSK IAM shape for AIOKafkaProducer / AIOKafkaConsumer."""
    return {
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "OAUTHBEARER",
        "sasl_oauth_token_provider": MskIamTokenProvider(KAFKA_MSK_REGION),
        "ssl_context": ssl.create_default_context(),
    }


def aiokafka_common_kwargs() -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
    }
    if KAFKA_USE_MSK_IAM:
        kwargs.update(_msk_iam_kwargs())
    else:
        kwargs["security_protocol"] = KAFKA_SECURITY_PROTOCOL
    return kwargs
