# app/main.py

import asyncio
from fastapi import FastAPI
from contextlib import asynccontextmanager

from backend.app.core.kafka_producer import kafka_producer
from backend.app.core.PolarisEngineNode import PolarisEngineNode, NodeState
import time


SERVER_ID = 0
TOTAL_NODES = 1
TOTAL_PARTITIONS = 64

consumer_manager = None


@asynccontextmanager
async def lifespan(app: FastAPI):

    global consumer_manager

    print(f"Polaris Node {SERVER_ID} booting...")

    await kafka_producer.connect()

    node_state = NodeState()

    consumer_manager = PolarisEngineNode(
        topic="all",
        server_id=SERVER_ID,
        state=node_state,
        total_nodes=TOTAL_NODES,
        total_partitions=TOTAL_PARTITIONS
    )
    
    consumer_task = asyncio.create_task(
        consumer_manager.start_listening()
    )

    yield

    print("Polaris shutting down...")

    consumer_task.cancel()
    await asyncio.gather(consumer_task, return_exceptions=True)

    await kafka_producer.disconnect()


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def root():

    return {
        "message": "Polaris API active",
        "node_id": SERVER_ID,
        "managed_partitions": consumer_manager.calculate_partition()
    }


# -------------------------
# TEST ENDPOINTS
# -------------------------


@app.post("/test/create-market")
async def test_create_market(market_id: str = "BTC-USD"):
    
    await kafka_producer.create_market(
        market_id,
        owner=f"Node_{SERVER_ID}"
    )

    # await asyncio.sleep(1)
    # print("markets", consumer_manager.state.markets)
    # print("orders", consumer_manager.state.order_books)
    # print(f"DEBUG: State ID is {id(consumer_manager.state)}")

    return {
        "status": "CREATE command sent",
        "market_id": market_id
    }

@app.post("/test/update-market")
async def test_update_market(market_id: str = "BTC-USD"):
    
    await kafka_producer.update_market(
        market_id,
        {"min_tick": 1}
    )

    # await asyncio.sleep(1)
    # print("markets", consumer_manager.state.markets)
    # print("orders", consumer_manager.state.order_books)
    # print(f"DEBUG: State ID is {id(consumer_manager.state)}")

    return {
        "status": "UPDATE command sent",
        "market_id": market_id
    }

@app.post("/test/place-order")
async def test_place_order(
    market_id: str = "BTC-USD",
    side: str = "BUY",
    price: float = 50000.0,
    volume: float = 1.0
):

    order_id = await kafka_producer.place_order(
        market_id=market_id,
        user_id="test_user",
        side=side.upper(),
        volume=volume,
        price=price
    )

    # await asyncio.sleep(1)
    # print("markets", consumer_manager.state.markets)
    # print("orders", consumer_manager.state.order_books)

    return {
        "status": "Order placed",
        "order_id": order_id
    }


@app.get("/test/inspect-book")
async def inspect_book(market_id: str = "BTC-USD"):

    state = consumer_manager.state
    book = state.order_books.get(
        market_id,
        {"bids": [], "asks": []}
    )

    # await asyncio.sleep(1)
    # print("markets", consumer_manager.state.markets)
    # print("orders", consumer_manager.state.order_books)

    return {
        "market_id": market_id,
        "known_markets": list(state.markets.keys()),
        "bids_count": len(book["bids"]),
        "asks_count": len(book["asks"]),
        "book": book
    }


@app.get("/test/inspect-market")
async def inspect_market(market_id: str = "BTC-USD"):
    market = consumer_manager.state.markets
    return market

@app.post("/test/trigger-sync")
async def trigger_sync(target_node_id: int = 0):

    # if target_node_id == SERVER_ID:
    #     return {"error": "Cannot sync with yourself"}

    my_partitions = consumer_manager.calculate_partition()
    
    current_markets = list(
        consumer_manager.state.markets.keys()
    )
    print(consumer_manager.state.order_books)

    print(f"Node {SERVER_ID} requesting sync from {target_node_id}")
    print("Known markets", consumer_manager.state.markets)

    await kafka_producer.request_state_sync(
        node_id=SERVER_ID,
        partitions=my_partitions,
        target_node=target_node_id
    )

    # await asyncio.sleep(1)
    # print("markets", consumer_manager.state.markets)
    # print("orders", consumer_manager.state.order_books)

    return {
        "status": "sync request sent",
        "requester": SERVER_ID,
        "target": target_node_id,
        "requested_partitions": my_partitions
    }