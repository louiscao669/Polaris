import asyncio
import json
from typing import Any, Optional

from aiokafka import AIOKafkaConsumer

from backend.app.core.kafka_consumer_sync import (
    sync_create_e,
    sync_create_m,
    sync_create_o,
    sync_create_o_role,
    sync_create_o_token,
    sync_designate_e_closed,
    sync_designate_e_contraint,
    sync_designate_e_market_creator,
    sync_designate_e_open_to,
    sync_designate_e_token,
    sync_designate_m_contraint,
    sync_designate_m_open_to_as,
    sync_designate_m_result,
    sync_designate_m_token,
    sync_do_m_payout,
    sync_do_m_transaction,
    sync_points_m,
    sync_stats_m_liquidity,
    sync_stats_m_time_focus,
    sync_stats_m_whales,
)
from backend.app.core.kafka_producer import (
    TOPIC_ORGANIZATION,
    TOPIC_PLATFORM_EVENT,
    TOPIC_PLATFORM_MARKET,
    TOPIC_PLATFORM_MARKET_ANALYTICS,
    TOPIC_PLATFORM_MARKET_FINANCE,
)

SUBSCRIBED_TOPICS = (
    TOPIC_ORGANIZATION,
    TOPIC_PLATFORM_EVENT,
    TOPIC_PLATFORM_MARKET,
    TOPIC_PLATFORM_MARKET_FINANCE,
    TOPIC_PLATFORM_MARKET_ANALYTICS,
)

ENGINE_TOPICS = SUBSCRIBED_TOPICS


class NodeState:
    """Optional in-process cache; Kafka handlers persist via kafka_consumer_sync."""

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
        self.consumer = AIOKafkaConsumer(
            bootstrap_servers="localhost:9092",
            group_id="polaris-engine",
            value_deserializer=lambda m: json.loads(m.decode()),
        )
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
        action = data.get("action")

        if topic == TOPIC_ORGANIZATION:
            if action == "CREATE_ORGANIZATION":
                await asyncio.to_thread(sync_create_o, data)
            elif action == "CREATE_ORGANIZATION_ROLE":
                await asyncio.to_thread(sync_create_o_role, data)
            elif action == "CREATE_ORGANIZATION_TOKEN":
                await asyncio.to_thread(sync_create_o_token, data)

        elif topic == TOPIC_PLATFORM_EVENT:
            if action == "CREATE_EVENT":
                await asyncio.to_thread(sync_create_e, data)
            elif action == "DESIGNATE_EVENT_TOKEN":
                await asyncio.to_thread(sync_designate_e_token, data)
            elif action == "DESIGNATE_EVENT_MARKET_CREATOR":
                await asyncio.to_thread(sync_designate_e_market_creator, data)
            elif action == "DESIGNATE_EVENT_CONSTRAINT":
                await asyncio.to_thread(sync_designate_e_contraint, data)
            elif action == "DESIGNATE_EVENT_OPEN_TO":
                await asyncio.to_thread(sync_designate_e_open_to, data)
            elif action == "DESIGNATE_EVENT_CLOSED":
                await asyncio.to_thread(sync_designate_e_closed, data)

        elif topic == TOPIC_PLATFORM_MARKET:
            if action == "CREATE_MARKET":
                await asyncio.to_thread(sync_create_m, data)
            elif action == "DESIGNATE_MARKET_TOKEN":
                await asyncio.to_thread(sync_designate_m_token, data)
            elif action == "DESIGNATE_MARKET_RESULT":
                await asyncio.to_thread(sync_designate_m_result, data)
            elif action == "DESIGNATE_MARKET_CONSTRAINT":
                await asyncio.to_thread(sync_designate_m_contraint, data)
            elif action == "DESIGNATE_MARKET_OPEN_TO_AS":
                await asyncio.to_thread(sync_designate_m_open_to_as, data)

        elif topic == TOPIC_PLATFORM_MARKET_FINANCE:
            if action == "MARKET_TRANSACTION":
                await asyncio.to_thread(sync_do_m_transaction, data)
            elif action == "MARKET_PAYOUT":
                await asyncio.to_thread(sync_do_m_payout, data)

        elif topic == TOPIC_PLATFORM_MARKET_ANALYTICS:
            if action == "STATS_LIQUIDITY":
                await asyncio.to_thread(sync_stats_m_liquidity, data)
            elif action == "STATS_TIME_FOCUS":
                await asyncio.to_thread(sync_stats_m_time_focus, data)
            elif action == "STATS_WHALES":
                await asyncio.to_thread(sync_stats_m_whales, data)
            elif action == "POINTS":
                await asyncio.to_thread(sync_points_m, data)
