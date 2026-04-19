# app/main.py

import asyncio
import os
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI

from .core.kafka_producer import kafka_producer
from .core.kafka_server import ENGINE_TOPICS, PolarisEngineNode, NodeState
from .database import create_db_and_tables
from .settings_app import (
    POLARIS_ENABLE_LEGACY_CONSUMER,
    POLARIS_ENABLE_V2_WORKER,
)
from .settings_worker import worker_topics_and_group
from .v2_kafka_client import v2_kafka_producer
from .v2_kafka_worker import PolarisV2Worker
from .v2_routes import router as v2_router
from dotenv import load_dotenv
load_dotenv()

SERVER_ID = 0
TOTAL_NODES = 1

consumer_manager: Optional[PolarisEngineNode] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global consumer_manager

    print(f"Polaris Node {SERVER_ID} booting...", flush=True)

    if os.getenv("POLARIS_BOOTSTRAP_DB", "").strip().lower() in {"1", "true", "yes"}:
        create_db_and_tables()

    await kafka_producer.connect()
    await v2_kafka_producer.connect()

    consumer_task: asyncio.Task | None = None
    v2_worker_task: asyncio.Task | None = None

    if POLARIS_ENABLE_LEGACY_CONSUMER:
        node_state = NodeState()
        consumer_manager = PolarisEngineNode(
            topic="all",
            server_id=SERVER_ID,
            state=node_state,
            total_servers=TOTAL_NODES,
        )
        consumer_task = asyncio.create_task(consumer_manager.start_listening())

    if POLARIS_ENABLE_V2_WORKER:
        topics, group_id = worker_topics_and_group()
        vw = PolarisV2Worker(topics, group_id)
        v2_worker_task = asyncio.create_task(vw.run_forever())

    yield

    print("Polaris shutting down...", flush=True)

    if consumer_task is not None:
        consumer_task.cancel()
        await asyncio.gather(consumer_task, return_exceptions=True)

    if v2_worker_task is not None:
        v2_worker_task.cancel()
        await asyncio.gather(v2_worker_task, return_exceptions=True)

    await kafka_producer.disconnect()
    await v2_kafka_producer.disconnect()


app = FastAPI(lifespan=lifespan)
app.include_router(v2_router)


@app.get("/")
async def root():
    legacy_topics = (
        list(ENGINE_TOPICS) if consumer_manager is not None else []
    )
    return {
        "message": "Polaris API",
        "node_id": SERVER_ID,
        "legacy_consumer_topics": legacy_topics,
        "legacy_consumer_enabled": POLARIS_ENABLE_LEGACY_CONSUMER,
        "v2_worker_embedded": POLARIS_ENABLE_V2_WORKER,
    }


@app.get("/health")
async def health():
    return {"status": "ok"}
