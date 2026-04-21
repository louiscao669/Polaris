import asyncio
import json
from typing import Any, Optional

from aiokafka import AIOKafkaConsumer

from .kafka_dispatch import dispatch_legacy_topic
from .kafka_producer import (
    TOPIC_ORGANIZATION,
    TOPIC_PLATFORM_EVENT,
    TOPIC_PLATFORM_MARKET,
    TOPIC_USER_IDENTITY,
)
from ..kafka_aiokafka_common import aiokafka_common_kwargs

SUBSCRIBED_TOPICS = (
    TOPIC_ORGANIZATION,
    TOPIC_PLATFORM_EVENT,
    TOPIC_PLATFORM_MARKET,
    TOPIC_USER_IDENTITY,
)

ENGINE_TOPICS = SUBSCRIBED_TOPICS


class NodeState:
    """Optional in-process cache; persistence uses ``kafka_handlers`` (Backend Functions)."""

    def __init__(self):
        self.organizations: dict[int, dict[str, Any]] = {}
        self.events: dict[int, dict[str, Any]] = {}
        self.platform_markets: dict[int, dict[str, Any]] = {}
        self.finance_log: list[dict[str, Any]] = []
        self.analytics_log: list[dict[str, Any]] = []


class PolarisEngineNode:
    def __init__(
        self,
        topic: str,
        server_id: int,
        state: NodeState,
        total_servers: int,
    ):
        self.topic = topic
        self.server_id = server_id
        self.total_nodes = total_servers
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.state = state

    async def start_listening(self) -> None:
        kwargs = aiokafka_common_kwargs()
        kwargs.update(
            {
                "group_id": "polaris-engine",
                "value_deserializer": lambda m: json.loads(m.decode()),
            }
        )

        self.consumer = AIOKafkaConsumer(**kwargs)
        await self.consumer.start()
        self.consumer.subscribe(list(SUBSCRIBED_TOPICS))

        try:
            async for msg in self.consumer:
                raw = msg.value
                if not isinstance(raw, dict):
                    continue
                await self._dispatch(msg.topic, raw)

        except asyncio.CancelledError:
            print(f"Node {self.server_id} shutdown")

        finally:
            if self.consumer is not None:
                await self.consumer.stop()

    async def _dispatch(self, topic: str, data: dict[str, Any]) -> None:
        await asyncio.to_thread(dispatch_legacy_topic, topic, data)
