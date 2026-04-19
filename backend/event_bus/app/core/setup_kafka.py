import asyncio
import ssl
from typing import Any

from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from ..msk_oauth import MskIamTokenProvider
from ..settings_kafka import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_MSK_REGION,
    KAFKA_SASL_MECHANISM,
    KAFKA_SECURITY_PROTOCOL,
    KAFKA_USE_MSK_IAM,
)

async def setup_polaris_topics():
    kwargs: dict[str, Any] = {"bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS}
    if KAFKA_USE_MSK_IAM:
        kwargs.update(
            {
                "security_protocol": KAFKA_SECURITY_PROTOCOL,
                "sasl_mechanism": KAFKA_SASL_MECHANISM,
                "sasl_oauth_token_provider": MskIamTokenProvider(KAFKA_MSK_REGION),
                "ssl_context": ssl.create_default_context(),
            }
        )
    else:
        kwargs["security_protocol"] = KAFKA_SECURITY_PROTOCOL

    admin_client = AIOKafkaAdminClient(**kwargs)
    
    try:
        await admin_client.start()
        
        topic_list = [
            "organization.lifecycle",
            "platform.event.lifecycle",
            "platform.market.lifecycle",
            "platform.market.finance",
            "platform.market.analytics",
            "user.identity.events",
            # v2 consolidated domains (+ DLQs)
            "market.operations",
            "market.operations.dlq",
            "event.lifecycle",
            "event.lifecycle.dlq",
            "org.management",
            "org.management.dlq",
            "user.account",
            "user.account.dlq",
        ]
        existing_topics = await admin_client.list_topics() 
        
        new_topics = []
        for topic in topic_list:
            if topic not in existing_topics:
                print(f"Creating {topic} with 64 partitions...")
                new_topics.append(NewTopic(name=topic, num_partitions=64, replication_factor=1))
            else:
                print(f"Topic {topic} already exists. Skipping.")

        if new_topics:
            await admin_client.create_topics(new_topics=new_topics)
            print("Success! All topics created.")
        else:
            print("All topics are already configured.")
            
    except Exception as e:
        print(f"Failed to setup Kafka: {e}")
        print("Tip: Make sure your Docker Kafka container is running and healthy!")
    finally:
        await admin_client.close()

if __name__ == "__main__":
    asyncio.run(setup_polaris_topics())