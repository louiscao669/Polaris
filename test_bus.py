import asyncio
import json
import uuid
import ssl
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.abc import AbstractTokenProvider  # Added this import
from sqlalchemy import create_engine, text
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

# --- CONFIGURATION ---
BOOTSTRAP_SERVERS = "b-2.polarisclusterv5.mtj5wq.c2.kafka.us-east-2.amazonaws.com:9098"
TOPIC = "MARKET_OPERATIONS"
REGION = "us-east-2" 
DB_URL = "mysql+pymysql://admin:yourpassword@polaris-db.clmsauq4mqfc.us-east-2.rds.amazonaws.com:3306/polaris"

# This class satisfies the "aiokafka.abc.AbstractTokenProvider" requirement
class MSKTokenProvider(AbstractTokenProvider):
    async def token(self):
        # The signer generates a tuple: (token_string, expiration_time_ms)
        token, _ = MSKAuthTokenProvider.generate_auth_token(REGION)
        return token

async def run_test():
    operation_id = str(uuid.uuid4())
    print(f"🚀 Starting Full Chain Test | ID: {operation_id}")

    sasl_ssl_context = ssl.create_default_context()
    
    # Create ONE instance of our new provider
    tp = MSKTokenProvider()

    # 1. SETUP PRODUCER
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        ssl_context=sasl_ssl_context,
        sasl_oauth_token_provider=tp  # Pass the object here
    )
    
    # 2. SETUP CONSUMER
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        ssl_context=sasl_ssl_context,
        sasl_oauth_token_provider=tp, # Pass the object here
        group_id=f"debug-group-{uuid.uuid4()}",
        auto_offset_reset="earliest"
    )

    print("🔌 Connecting to MSK with IAM Token Provider...")
    try:
        await producer.start()
        await consumer.start()
        print("✅ Connected to MSK!")

        # ... rest of the script (Produce, Consume, DB check) ...
        # [Keeping the same logic from previous version below]
        test_payload = {"oid": operation_id, "action": "test_ping"}
        print(f"📤 Sending message...")
        await producer.send_and_wait(TOPIC, json.dumps(test_payload).encode("utf-8"))
        print("✅ Written to MSK.")

        print("📥 Waiting for Consumer...")
        async for msg in consumer:
            data = json.loads(msg.value.decode("utf-8"))
            if data.get("oid") == operation_id:
                print(f"✅ Consumer received message!")
                break
        
        engine = create_engine(DB_URL)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            print("✅ DB Connection: OK")
        print("\n🏆 SYSTEM IS FUNCTIONAL!")

    except Exception as e:
        print(f"\n❌ FAILED: {type(e).__name__}: {e}")
    finally:
        await producer.stop()
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(run_test())
