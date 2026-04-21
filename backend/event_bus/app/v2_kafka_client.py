"""Shared aiokafka producer configuration for v2 async writes with MSK IAM."""

from __future__ import annotations

import json
import ssl
from typing import Any, Optional

from aiokafka import AIOKafkaProducer
from aiokafka.abc import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

from .kafka_aiokafka_common import aiokafka_common_kwargs

# 1. IAM Token Provider Class (Required for aiokafka + MSK IAM)
class MSKTokenProvider(AbstractTokenProvider):
    def __init__(self, region: str = "us-east-2"):
        self.region = region

    async def token(self):
        # Generates the signed IAM token using the EC2's Instance Profile
        token, _ = MSKAuthTokenProvider.generate_auth_token(self.region)
        return token

class V2KafkaProducer:
    def __init__(self) -> None:
        self._producer: Optional[AIOKafkaProducer] = None

    async def connect(self) -> None:
        # Get base settings (brokers, etc.)
        kwargs = aiokafka_common_kwargs()
        
        # 2. Force Security Settings for MSK IAM Port 9098
        kwargs.update(
            {
                "security_protocol": "SASL_SSL",
                "sasl_mechanism": "OAUTHBEARER",
                "sasl_oauth_token_provider": MSKTokenProvider(),
                "ssl_context": ssl.create_default_context(),
                "enable_idempotence": True,
                "value_serializer": lambda v: json.dumps(v, default=str).encode(),
            }
        )

        prod = AIOKafkaProducer(**kwargs)
        try:
            print("📡 Connecting Producer to MSK with IAM...")
            await prod.start()
            print("✅ Producer Connected Successfully")
        except BaseException as e:
            print(f"❌ Producer Failed to Start: {e}")
            try:
                await prod.stop()
            except Exception:
                pass
            raise
        self._producer = prod

    async def disconnect(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

    def _require(self) -> AIOKafkaProducer:
        if self._producer is None:
            raise RuntimeError("v2 kafka producer not connected")
        return self._producer

    async def send_json(
        self,
        *,
        topic: str,
        value: dict[str, Any],
        key: bytes | None,
    ) -> tuple[int, int]:
        # This sends the message and waits for MSK acknowledgement
        md = await self._require().send_and_wait(topic, value, key=key)
        return md.partition, md.offset


v2_kafka_producer = V2KafkaProducer()