"""Shared aiokafka consumer/producer SSL + MSK IAM kwargs.

When ``KAFKA_USE_MSK_IAM`` is true, this matches AWS guidance for Python + aiokafka:

.. code-block:: text

    security_protocol=SASL_SSL
    sasl_mechanism=OAUTHBEARER
    sasl_oauth_token_provider wraps MSKAuthTokenProvider.generate_auth_token(region)

Do **not** set ``sasl_mechanism`` to ``AWS_MSK_IAM`` here — that is not an aiokafka value.
IAM auth still uses AWS credentials from the environment/instance/task role.
"""

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
                # OAUTHBEARER + token provider is the Python/aiokafka equivalent of MSK IAM.
                "sasl_mechanism": KAFKA_SASL_MECHANISM,
                "sasl_oauth_token_provider": MskIamTokenProvider(KAFKA_MSK_REGION),
                "ssl_context": ssl.create_default_context(),
            }
        )
    else:
        kwargs["security_protocol"] = KAFKA_SECURITY_PROTOCOL
    return kwargs
