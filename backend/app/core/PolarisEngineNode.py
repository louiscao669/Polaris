from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
import zlib
import json
import asyncio
from collections import defaultdict
import heapq
import time


class NodeState:
    def __init__(self):
        self.markets = {}

        # Heap based order books
        self.order_books = defaultdict(
            lambda: {"bids": [], "asks": []}
        )


class PolarisEngineNode:
    
    def __init__(self, topic, server_id, state: NodeState, total_nodes, total_partitions=64):

        self.topic = topic
        self.server_id = server_id
        self.total_nodes = total_nodes
        self.total_partitions = total_partitions

        self.consumer = None
        self.producer = None

        self.state = state
        self.node_registry = {}

        self.my_partitions = set()

    def _get_partition_for_market(self, market_id): # calculate of the partition the market in the incoming message belongs to

        hash_value = zlib.crc32(market_id.encode()) & 0xffffffff
        return hash_value % self.total_partitions

    def calculate_partition(self): # calculate the partitions this node is responsible

        step = self.total_partitions // self.total_nodes
        start = self.server_id * step

        if self.server_id == self.total_nodes - 1:
            end = self.total_partitions
        else:
            end = start + step

        return list(range(start, end))

    def _should_process(self, msg): # see if this node is responsible for the message

        data = msg.value

        if msg.topic in ["market_metadata", "sys.control", "sys.heartbeats"]: # these messages are broadcast in the current system, need to be improved to an actual chord system
            return True

        if "market_id" in data:

            target_p = self._get_partition_for_market(data["market_id"]) 

            if target_p not in self.my_partitions: 
                return False

        return True

    async def start_listening(self):

        self.producer = AIOKafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode()
        )
        await self.producer.start()

        self.consumer = AIOKafkaConsumer(
            bootstrap_servers="localhost:9092",
            value_deserializer=lambda m: json.loads(m.decode())
        )

        await self.consumer.start()

        self.my_partitions = set(self.calculate_partition())

        assignment = []

        for topic in ["orders", "prices", "market_metadata"]:
            for p in self.my_partitions:
                assignment.append(TopicPartition(topic, p)) 

        assignment.append(TopicPartition("sys.control", 0))
        assignment.append(TopicPartition("sys.heartbeats", 0))
        self.consumer.assign(assignment)   # assign the consumer with all the topics
        # if it is a broadcast topic, assigned to partition 0; otherwise, assigned to every partition it is responsible for
        try:

            async for msg in self.consumer:

                if not self._should_process(msg):
                    continue

                if msg.topic == "orders":
                    await self._process_orders(msg.value)

                elif msg.topic == "prices":
                    await self._process_price(msg.value)

                elif msg.topic == "market_metadata":
                    await self._process_metadata(msg.value)
                
                elif msg.topic == "sys.control":
                    await self._process_control(msg.value)

                elif msg.topic == "sys.heartbeats":
                    await self._process_heartbeat(msg.value)


        except asyncio.CancelledError:
            print(f"Node {self.server_id} shutdown")

        finally:
            await self.consumer.stop()
            await self.producer.stop()

    async def _process_orders(self, data):
        market_id = data["market_id"]
        side = data["side"]

        if data.get("action") == "CANCEL":       # cancel the order

            order_id = data["order_id"]
            book = self.state.order_books[market_id]
            
            for side_book in ["bids", "asks"]:
                book[side_book] = [
                    o for o in book[side_book] if o[2]["order_id"] != order_id
                ]
            
            heapq.heapify(book["bids"])
            heapq.heapify(book["asks"])
            return
        elif data.get("action") == "PLACE":      # place an order
            if side == "BUY":
                await self._match_or_store(market_id, data, True)
            else:
                await self._match_or_store(market_id, data, False)

    async def _match_or_store(self, market_id, order, is_bid):
        
        book = self.state.order_books[market_id]     # get the order book of this market

        bids = book["bids"]
        asks = book["asks"]

        price = order["price"]
        timestamp = order["timestamp"]

        if is_bid:          # it is an Buy order

            while asks and price >= asks[0][0]:    # there is a Sell order and buy price is better than best sell price

                best_price, ts, best_order = heapq.heappop(asks)

                print(f"Match {market_id} BUY {price} vs SELL {best_price}")

                return
            heapq.heappush(bids, (-price, timestamp, order))
            print("bids:", bids)

        else:

            while bids and price <= -bids[0][0]:

                best_price, ts, best_order = heapq.heappop(bids)

                print(f"Match {market_id} SELL {price} vs BUY {-best_price}")

                return

            heapq.heappush(asks, (price, timestamp, order))

    async def _process_metadata(self, data):

        action = data.get("action")
        market_id = data.get("market_id")

        if action == "CREATE":   # create a market

            self.state.markets[market_id] = data
            print(f"DEBUG: State ID is {id(self.state)}")
            # print(self.state.markets)

        elif action == "DELETE":    # delete a market

            self.state.markets.pop(market_id, None)
            self.state.order_books.pop(market_id, None)
        
        elif action == "UPDATE":
            for key, value in data.items():
                if key == "owner":
                    if isinstance(value, str): self.state.markets[market_id][key] = value
                    else: print("Update value of the wrong type")
                elif key in ["timestamp", "last_update", "min_tick"]:
                    if isinstance(value, (int, float)): self.state.markets[market_id][key] = value
                    else: print("Update value of the wrong type")

    async def _process_price(self, data):     # process the price change for the entire market

        market_id = data.get("market_id")

        if market_id in self.state.markets:

            self.state.markets[market_id]["last_price"] = data["price"]
            self.state.markets[market_id]["last_update"] = data["timestamp"]

    async def _process_control(self, data):     # process a new node joining

        action = data.get("action")

        if action == "SYNC_REQUEST":
            await self._process_sync_request(data)

        elif action == "SYNC_RESPONSE":
            await self._apply_sync_data(data["data"], data["partition"])

        elif action == "HANDOFF_COMPLETE":
            await self._process_handoff_complete(data)

    async def _process_sync_request(self, data):

        target_partitions = data["partitions"]

        state_to_send = {}

        for m_id in self.state.markets:

            if self._get_partition_for_market(m_id) in target_partitions:

                state_to_send[m_id] = self.state.order_books[m_id]

        payload = {
            "action": "SYNC_RESPONSE",
            "receiver_id": data["server_id"],
            "partition" : target_partitions,
            "data": state_to_send
        }

        await self.producer.send_and_wait("sys.control", payload)

    async def _apply_sync_data(self, incoming_state, partitions):

        for market_id, book in incoming_state.items():
            self.state.order_books[market_id] = book

        print(f"Node {self.server_id} loaded {len(incoming_state)} markets")

        handoff_payload = {
            "action": "HANDOFF_COMPLETE",
            "server_id": self.server_id,
            "partitions": partitions
        }
        await self.producer.send_and_wait("sys.control", handoff_payload)

    async def _process_handoff_complete(self, data):
        # 1. Identity Guard: Never process your own handoff message
        if data["server_id"] == self.server_id:
            return

        partitions_to_clear = data["partitions"]
        
        # 2. Safety Guard: Check if I am still assigned these partitions
        # If I still have them in my_partitions, someone is trying to 
        # steal them incorrectly or the message is stale.
        for p in partitions_to_clear:
            if p in self.my_partitions:
                print(f"Node {self.server_id}: Blocking deletion of partition {p} - I still own it!")
                return

        # 3. Targeted Deletion
        for market_id in list(self.state.order_books.keys()):
            if self._get_partition_for_market(market_id) in partitions_to_clear:
                self.state.order_books.pop(market_id, None)
                print(f"Node {self.server_id}: Successfully handed off {market_id}")

    async def _process_heartbeat(self, data):

        node_id = data["node_id"]

        if node_id == self.server_id:
            return

        self.node_registry[node_id] = {
            "last_seen": data["timestamp"],
            "load": data["load"],
            "status": data["status"]
        }

        now = time.time_ns()

        for nid, info in list(self.node_registry.items()):

            if now - info["last_seen"] > 30 * 1e9:
                print(f"Node {self.server_id}: Node {nid} might be down")