import zlib
import uuid
import json
import time
from aiokafka import AIOKafkaProducer


class KafkaProducerManager:

    def __init__(self, total_partitions=64):
        self.producer = None
        self.total_partitions = total_partitions

    def _get_partition(self, market_id):    # get the partition manually so that the message is sent to the nodes that are assigned to the markets (match _get_partition_for_market() in kafka_consumer)
        hash_value = zlib.crc32(market_id.encode()) & 0xffffffff
        return hash_value % self.total_partitions

    def _timestamp(self):
        return time.time_ns()

    async def connect(self):

        self.producer = AIOKafkaProducer(
            bootstrap_servers="localhost:9092",
            enable_idempotence=True,
            value_serializer=lambda v: json.dumps(v).encode()
        )

        await self.producer.start()

    async def disconnect(self):
        await self.producer.stop()

    async def create_market(self, market_id, owner, min_tick = 0.01):

        payload = {
            "action": "CREATE",
            "market_id": market_id,
            "owner": owner,
            "min_tick": min_tick,
            "timestamp": self._timestamp()
        }

        await self.producer.send_and_wait(
            "market_metadata",
            payload,
            partition=self._get_partition(market_id)
        )

    async def update_market(self, market_id, updates):

        payload = {
            "action": "UPDATE",
            "market_id": market_id,
            **updates,
            "timestamp": self._timestamp()
        }

        await self.producer.send_and_wait(
            "market_metadata",
            payload,
            partition=self._get_partition(market_id)
        )

    async def place_order(self, market_id, user_id, side, volume, price):

        order_id = str(uuid.uuid4())

        payload = {
            "action": "PLACE",
            "order_id": order_id,
            "market_id": market_id,
            "user_id": user_id,
            "side": side,
            "volume": volume,
            "price": price,
            "timestamp": self._timestamp()
        }

        await self.producer.send_and_wait(
            "orders",
            payload,
            partition=self._get_partition(market_id)
        )

        return order_id

    async def cancel_order(self, order_id, market_id):

        payload = {
            "action": "CANCEL",
            "order_id": order_id,
            "market_id": market_id,
            "timestamp": self._timestamp()
        }

        await self.producer.send_and_wait(
            "orders",
            payload,
            partition=self._get_partition(market_id)
        )

    async def update_price(self, market_id, price):
        """
        Broadcasts the latest traded price to the entire cluster.
        """
        payload = {
            "market_id": market_id,
            "price": price,
            "timestamp": self._timestamp()
        }

        await self.producer.send_and_wait(
            "prices",
            payload,
            partition=self._get_partition(market_id)
        )

    async def emit_heartbeat(self, node_id, load):

        payload = {
            "node_id": node_id,
            "status": "ALIVE",
            "load": load,
            "timestamp": self._timestamp()
        }

        await self.producer.send_and_wait(
            "sys.heartbeats",
            payload,
            partition=0
        )

    async def request_state_sync(self, node_id, partitions, target_node=None):

        payload = {
            "action": "SYNC_REQUEST",
            "server_id": node_id,
            "partitions": partitions,
            "receiver_id": target_node,
            "timestamp": self._timestamp()
        }

        await self.producer.send_and_wait("sys.control", payload, partition=0)

    async def acknowledge_handoff(self, node_id, partitions):

        payload = {
            "action": "HANDOFF_COMPLETE",
            "server_id": node_id,
            "partitions": partitions
        }

        await self.producer.send_and_wait("sys.control", payload, partition=0)


kafka_producer = KafkaProducerManager()