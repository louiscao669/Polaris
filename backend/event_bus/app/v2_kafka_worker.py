"""Kafka consumer workers for v2 consolidated topics (envelope messages)."""

from __future__ import annotations

import asyncio
import json
import ssl
import traceback
from typing import Any
from uuid import UUID

from aiokafka import AIOKafkaConsumer
from aiokafka.abc import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

from .core.kafka_dispatch import dispatch_v2_consolidated
from .kafka_aiokafka_common import aiokafka_common_kwargs
from .operations_repo import update_operation_status
from .processed_events_repo import classify_message, mark_applied, mark_failed
from .topics import dlq_for
from .v2_kafka_client import v2_kafka_producer

async def send_to_dlq(
    source_topic: str,
    envelope: dict[str, Any],
    error: str,
    tb: str = "",
) -> None:
    dlq_topic = dlq_for(source_topic)
    if dlq_topic is None:
        return
    payload: dict[str, Any] = {
        "error": error,
        "traceback": (tb or "")[-8000:],
        "original": envelope,
    }
    await v2_kafka_producer.send_json(topic=dlq_topic, value=payload, key=None)


def _apply_envelope(topic: str, envelope: dict[str, Any]) -> None:
    payload = envelope.get("payload")
    if not isinstance(payload, dict):
        raise ValueError("envelope.payload must be an object")
    return dispatch_v2_consolidated(topic, payload)


async def process_kafka_message(msg: Any, consumer_group: str) -> None:
    raw = msg.value
    if not isinstance(raw, dict):
        return

    meta = raw.get("metadata")
    if not isinstance(meta, dict):
        raise ValueError("invalid envelope: missing metadata object")

    event_id = meta.get("event_id")
    if not event_id:
        raise ValueError("metadata.event_id is required")

    event_id_str = str(event_id)

    outcome = classify_message(
        event_id=event_id_str,
        consumer_group=consumer_group,
        topic=msg.topic,
        partition=msg.partition,
        offset=msg.offset,
    )
    if outcome == "skip_done":
        return

    oid = UUID(event_id_str)
    update_operation_status(operation_id=oid, status="processing")

    try:
        result = await asyncio.to_thread(_apply_envelope, msg.topic, raw)
    except Exception as e:
        err = str(e) or repr(e)
        tb = traceback.format_exc()
        mark_failed(
            event_id=event_id_str,
            consumer_group=consumer_group,
            error_message=err,
        )
        update_operation_status(
            operation_id=oid, status="failed", error_message=err
        )
        await send_to_dlq(msg.topic, raw, err, tb)
        return

    mark_applied(event_id=event_id_str, consumer_group=consumer_group)
    update_operation_status(operation_id=oid, status="succeeded", result=result)


class MSKTokenProvider(AbstractTokenProvider):
    def __init__(self, region: str = "us-east-2"):
        self.region = region

    async def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(self.region)
        return token


class PolarisV2Worker:
    def __init__(self, topics: list[str], group_id: str) -> None:
        self.topics = topics
        self.group_id = group_id
        self.consumer: AIOKafkaConsumer | None = None

    async def run_forever(self) -> None:
        kwargs = aiokafka_common_kwargs()
        kwargs.update(
            {
                "group_id": self.group_id,
                "security_protocol": "SASL_SSL",
                "sasl_mechanism": "OAUTHBEARER",
                "sasl_oauth_token_provider": MSKTokenProvider(),
                "ssl_context": ssl.create_default_context(),
                "value_deserializer": lambda m: json.loads(m.decode()),
            }
        )

        self.consumer = AIOKafkaConsumer(**kwargs)
        print(f"📡 Worker connecting to MSK...")
        await self.consumer.start()
        self.consumer.subscribe(self.topics)

        try:
            async for msg in self.consumer:
                await process_kafka_message(msg, self.group_id)
        finally:
            if self.consumer:
                await self.consumer.stop()
