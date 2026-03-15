To run this program:

docker-compose up -d
python setup_kafka.py
uvicorn backend.app.main:app --reload 

Then go to http://127.0.0.1:8000/docs, this will lead to the FastAPI page where we can test the different functions in main.py. 
Then run create-market -> place-order -> inspect-book -> trigger-sync
When make any modifications in main.py, kafka_producer.py or kafka_consumer, 
Polaris Node 0 booting...
INFO:     Application startup complete
will be triggered, and the data in the nodes will be lost, as reflected when we rerun the test functions in the fastAPI page. So a way is needed to log the data so that the messages are not lost

setup_kafka.py
This file sets up the desired amount of partitions (e.g. 64) manually for every topic on the event bus

kafka_producer.py
This file creates the producer that publishes the messages onto the topics. Specifically,
the request_state_sync is used to sync the state of newly joint nodes by asking its neighbors
acknowledge_handoff is used to tell the nodes the handoff is complete so that the node previously owning the partitions can let go of them

PolarisEngineNode.py
This file creates the server node that is both a consumer and a producer. It listens for messages on the event bus and process the incoming messages it is responsible for (consumer), it can also publish messages onto the event bus (producer)

main.py
This file currently tests the different kafka consumer/producer's ability to publish and subscribe to topics and process the messages

When a new node joins, producer sends a request_state_sync message to a target node. When the target node receives this request, it runs process_sync_request that publishes the data that needs to be synced, the requesting node receives the data and runs _apply_sync_data and sends a message to _process_handoff_complete. Finally, the target node receives the handoff_complete message and delete the markets assigned to the requesting nodes