from aiokafka import AIOKafkaConsumer
from app import settings
import asyncio
import logging

logger = logging.getLogger(__name__)

MAX_RETRIES = 5
RETRY_INTERVAL = 10  # seconds

async def consume_products(topic, bootstrap_servers, group_id):
    retries = 0
    
    while retries < MAX_RETRIES:
        try:
            consumer = AIOKafkaConsumer(
                topic, 
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
                auto_offset_reset='earliest',
            )

            logger.info("Consumer created, attempting to start...")
            await consumer.start()
            logger.info("Consumer started successfully")
            break
        except KafkaConnectionError as e:
            retries += 1
            logger.error(f"Kafka connection error: {e}")
            logger.info(f"Retrying {retries}/{MAX_RETRIES}...")
            await asyncio.sleep(RETRY_INTERVAL)
    else:
        logger.error("Failed to connect to Kafka broker after several retries")
        return

    try:
        async for msg in consumer:
            logger.info(f"Received message on topic: {msg.topic}")
            logger.info(f"Message Value: {msg.value}")
            # logger.info(f"Message key: {msg.key}")

        
    except KafkaError as e:
        logger.error(f"Error while consuming message: {e}")
    finally:
        logger.info("Stopping consumer")
        await consumer.stop()