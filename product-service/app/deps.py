from aiokafka import AIOKafkaProducer
from sqlmodel import Session
from app.database import engine
from app.settings import BOOTSTRAP_SERVER


# Kafka Producer as a dependency
async def get_kafka_producer():
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()


def get_session():
    with Session(engine) as session:
        yield session