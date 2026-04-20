"""
When ``KAFKA_USE_MSK_IAM=true``, ``connect()`` builds the same initialization as::

    # Sync token (see ``app.msk_oauth.get_msk_auth_token``).
    # aiokafka requires AbstractTokenProvider; we use ``MskIamTokenProvider``.

This module merges that with ``enable_idempotence`` and ``value_serializer`` via
``kafka_aiokafka_common.aiokafka_common_kwargs()``.
"""

import json
import time
from typing import Any, Optional

from aiokafka import AIOKafkaProducer

from ..kafka_aiokafka_common import aiokafka_common_kwargs

# Topics for multi_server domain parity (see multi_server/Backend Functions)
TOPIC_ORGANIZATION = "organization.lifecycle"
TOPIC_PLATFORM_EVENT = "platform.event.lifecycle"
TOPIC_PLATFORM_MARKET = "platform.market.lifecycle"
TOPIC_USER_IDENTITY = "user.identity.events"


class KafkaProducerManager:
    def __init__(self) -> None:
        self.producer: Optional[AIOKafkaProducer] = None

    def _timestamp(self) -> int:
        return time.time_ns()

    def _require_producer(self) -> AIOKafkaProducer:
        if self.producer is None:
            raise RuntimeError("Kafka producer not connected; call connect() first")
        return self.producer

    async def _send(
        self,
        topic: str,
        payload: dict[str, Any],
        *,
        partition: Optional[int] = None,
        key: Optional[bytes] = None,
    ) -> None:
        kwargs: dict[str, Any] = {}
        if partition is not None:
            kwargs["partition"] = partition
        if key is not None:
            kwargs["key"] = key
        await self._require_producer().send_and_wait(topic, payload, **kwargs)

    async def connect(self) -> None:
        # Same kwargs as tutorial MSK IAM producer; token from get_msk_auth_token via
        # MskIamTokenProvider — see kafka_aiokafka_common / msk_oauth.
        kwargs = aiokafka_common_kwargs()
        kwargs.update(
            {
                "enable_idempotence": True,
                "value_serializer": lambda v: json.dumps(v).encode(),
            }
        )

        prod = AIOKafkaProducer(**kwargs)
        try:
            await prod.start()
        except BaseException:
            try:
                await prod.stop()
            except Exception:
                pass
            raise
        self.producer = prod

    async def disconnect(self) -> None:
        if self.producer is not None:
            await self.producer.stop()
            self.producer = None

    # --- Organization (organization.py) ---

    async def create_o(
        self, user_id: int, name: str, description: str, organization_id: int
    ) -> None:
        await self._send(
            TOPIC_ORGANIZATION,
            {
                "action": "CREATE_ORGANIZATION",
                "user_id": user_id,
                "name": name,
                "description": description,
                "organization_id": organization_id,
                "timestamp": self._timestamp(),
            },
            key=str(organization_id).encode(),
        )

    async def create_o_role(
        self, user_id: int, organization_id: int, name: str, desc: str
    ) -> None:
        await self._send(
            TOPIC_ORGANIZATION,
            {
                "action": "CREATE_ORGANIZATION_ROLE",
                "user_id": user_id,
                "organization_id": organization_id,
                "name": name,
                "desc": desc,
                "timestamp": self._timestamp(),
            },
            key=str(organization_id).encode(),
        )

    async def create_o_token(
        self,
        user_id: int,
        organization_id: int,
        token_name: str,
        token_id: int,
        description: Optional[str] = None,
    ) -> None:
        await self._send(
            TOPIC_ORGANIZATION,
            {
                "action": "CREATE_ORGANIZATION_TOKEN",
                "user_id": user_id,
                "organization_id": organization_id,
                "token_name": token_name,
                "token_id": token_id,
                "description": description,
                "timestamp": self._timestamp(),
            },
            key=str(organization_id).encode(),
        )

    # --- Prediction-market events (event_logic.py) ---

    async def create_e(
        self, user_id: int, organization_id: int, caption: str, event_id: int
    ) -> None:
        await self._send(
            TOPIC_PLATFORM_EVENT,
            {
                "action": "CREATE_EVENT",
                "user_id": user_id,
                "organization_id": organization_id,
                "caption": caption,
                "event_id": event_id,
                "timestamp": self._timestamp(),
            },
            key=str(event_id).encode(),
        )

    async def designate_e_token(
        self, user_id: int, event_id: int, token_id: int
    ) -> None:
        await self._send(
            TOPIC_PLATFORM_EVENT,
            {
                "action": "DESIGNATE_EVENT_TOKEN",
                "user_id": user_id,
                "event_id": event_id,
                "token_id": token_id,
                "timestamp": self._timestamp(),
            },
            key=str(event_id).encode(),
        )

    async def designate_e_market_creator(
        self, user_id: int, event_id: int, market_creator_id: int
    ) -> None:
        await self._send(
            TOPIC_PLATFORM_EVENT,
            {
                "action": "DESIGNATE_EVENT_MARKET_CREATOR",
                "user_id": user_id,
                "event_id": event_id,
                "market_creator_id": market_creator_id,
                "timestamp": self._timestamp(),
            },
            key=str(event_id).encode(),
        )

    async def designate_e_contraint(
        self, user_id: int, event_id: int, constraint_id: int, value: Any
    ) -> None:
        await self._send(
            TOPIC_PLATFORM_EVENT,
            {
                "action": "DESIGNATE_EVENT_CONSTRAINT",
                "user_id": user_id,
                "event_id": event_id,
                "constraint_id": constraint_id,
                "value": value,
                "timestamp": self._timestamp(),
            },
            key=str(event_id).encode(),
        )

    async def designate_e_open_to(
        self, user_id: int, event_id: int, role_id: int
    ) -> None:
        await self._send(
            TOPIC_PLATFORM_EVENT,
            {
                "action": "DESIGNATE_EVENT_OPEN_TO",
                "user_id": user_id,
                "event_id": event_id,
                "role_id": role_id,
                "timestamp": self._timestamp(),
            },
            key=str(event_id).encode(),
        )

    async def designate_e_closed(self, user_id: int, event_id: int) -> None:
        await self._send(
            TOPIC_PLATFORM_EVENT,
            {
                "action": "DESIGNATE_EVENT_CLOSED",
                "user_id": user_id,
                "event_id": event_id,
                "timestamp": self._timestamp(),
            },
            key=str(event_id).encode(),
        )

    # --- Markets (market_logic.py) — lifecycle ---

    async def create_m(
        self,
        user_id: int,
        event_id: int,
        question: str,
        description: str,
        market_id: int,
    ) -> None:
        await self._send(
            TOPIC_PLATFORM_MARKET,
            {
                "action": "CREATE_MARKET",
                "user_id": user_id,
                "event_id": event_id,
                "question": question,
                "description": description,
                "market_id": market_id,
                "timestamp": self._timestamp(),
            },
            key=str(market_id).encode(),
        )

    async def designate_m_token(
        self, user_id: int, market_id: int, token_id: int
    ) -> None:
        await self._send(
            TOPIC_PLATFORM_MARKET,
            {
                "action": "DESIGNATE_MARKET_TOKEN",
                "user_id": user_id,
                "market_id": market_id,
                "token_id": token_id,
                "timestamp": self._timestamp(),
            },
            key=str(market_id).encode(),
        )

    async def designate_m_result(
        self, user_id: int, market_id: int, result: Any
    ) -> None:
        await self._send(
            TOPIC_PLATFORM_MARKET,
            {
                "action": "DESIGNATE_MARKET_RESULT",
                "user_id": user_id,
                "market_id": market_id,
                "result": result,
                "timestamp": self._timestamp(),
            },
            key=str(market_id).encode(),
        )

    async def designate_m_contraint(
        self, user_id: int, market_id: int, constraint_id: int, value: Any
    ) -> None:
        await self._send(
            TOPIC_PLATFORM_MARKET,
            {
                "action": "DESIGNATE_MARKET_CONSTRAINT",
                "user_id": user_id,
                "market_id": market_id,
                "constraint_id": constraint_id,
                "value": value,
                "timestamp": self._timestamp(),
            },
            key=str(market_id).encode(),
        )

    async def designate_m_open_to_as(
        self, user_id: int, market_id: int, role_id: int, as_id: int
    ) -> None:
        await self._send(
            TOPIC_PLATFORM_MARKET,
            {
                "action": "DESIGNATE_MARKET_OPEN_TO_AS",
                "user_id": user_id,
                "market_id": market_id,
                "role_id": role_id,
                "as_id": as_id,
                "timestamp": self._timestamp(),
            },
            key=str(market_id).encode(),
        )

    # --- Markets — finance (do_m_transaction uses `type` as price in SQL layer) ---

    async def do_m_transaction(
        self,
        user_id: int,
        market_id: int,
        token_id: int,
        type: int,
        side: Any,
        qty: Any,
        transaction_id: int,
    ) -> None:
        await self._send(
            TOPIC_PLATFORM_MARKET,
            {
                "action": "MARKET_TRANSACTION",
                "user_id": user_id,
                "market_id": market_id,
                "token_id": token_id,
                "type": type,
                "side": side,
                "qty": qty,
                "transaction_id": transaction_id,
                "timestamp": self._timestamp(),
            },
            key=str(market_id).encode(),
        )

    async def do_m_payout(
        self, user_id: int, market_id: int, token_id: int
    ) -> None:
        await self._send(
            TOPIC_PLATFORM_MARKET,
            {
                "action": "MARKET_PAYOUT",
                "user_id": user_id,
                "market_id": market_id,
                "token_id": token_id,
                "timestamp": self._timestamp(),
            },
            key=str(market_id).encode(),
        )

kafka_producer = KafkaProducerManager()
