"""Standalone entrypoint: v2 Kafka consumer worker (no HTTP)."""

from __future__ import annotations

import asyncio
import os
from dotenv import load_dotenv
load_dotenv()
from backend.app.database import create_db_and_tables
from backend.app.settings_worker import worker_topics_and_group
from backend.app.v2_kafka_client import v2_kafka_producer
from backend.app.v2_kafka_worker import PolarisV2Worker


async def main() -> None:
    if os.getenv("POLARIS_BOOTSTRAP_DB", "").strip().lower() in {"1", "true", "yes"}:
        create_db_and_tables()

    topics, group_id = worker_topics_and_group()
    await v2_kafka_producer.connect()
    worker = PolarisV2Worker(topics, group_id)
    await worker.run_forever()


if __name__ == "__main__":
    asyncio.run(main())
