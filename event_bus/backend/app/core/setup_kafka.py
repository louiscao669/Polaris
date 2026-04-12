import asyncio
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

async def setup_polaris_topics():
    # Using aiokafka's admin client instead
    admin_client = AIOKafkaAdminClient(bootstrap_servers="localhost:9092") # admin client for the event bus
    
    try:
        await admin_client.start()
        
        topic_list = ["orders", "market_metadata", "prices", "sys.control"]
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