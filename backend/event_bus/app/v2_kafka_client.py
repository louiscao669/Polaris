"""Shared aiokafka producer configuration for v2 async writes."""

from __future__ import annotations

import json
from typing import Any, Optional

from aiokafka import AIOKafkaProducer

from .kafka_aiokafka_common import aiokafka_common_kwargs


class V2KafkaProducer:
    def __init__(self) -> None:
        self._producer: Optional[AIOKafkaProducer] = None

    async def connect(self) -> None:
        kwargs = aiokafka_common_kwargs()
        kwargs.update(
            {
                "enable_idempotence": True,
                "value_serializer": lambda v: json.dumps(v, default=str).encode(),
            }
        )

        self._producer = AIOKafkaProducer(**kwargs)
        await self._producer.start()

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
        md = await self._require().send_and_wait(topic, value, key=key)
        return md.partition, md.offset


v2_kafka_producer = V2KafkaProducer()
