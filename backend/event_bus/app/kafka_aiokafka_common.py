"""Shared aiokafka consumer/producer SSL + MSK IAM kwargs."""

from __future__ import annotations

import ssl
from typing import Any

from .msk_oauth import MskIamTokenProvider
from .settings_kafka import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_MSK_REGION,
    KAFKA_SASL_MECHANISM,
    KAFKA_SECURITY_PROTOCOL,
    KAFKA_USE_MSK_IAM,
)


def aiokafka_common_kwargs() -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
    }
    if KAFKA_USE_MSK_IAM:
        kwargs.update(
            {
                "security_protocol": KAFKA_SECURITY_PROTOCOL,
                "sasl_mechanism": KAFKA_SASL_MECHANISM,
                "sasl_oauth_token_provider": MskIamTokenProvider(KAFKA_MSK_REGION),
                "ssl_context": ssl.create_default_context(),
            }
        )
    else:
        kwargs["security_protocol"] = KAFKA_SECURITY_PROTOCOL
    return kwargs
