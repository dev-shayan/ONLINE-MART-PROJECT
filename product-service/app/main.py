from fastapi import FastAPI
from sqlmodel import Field, SQLModel
from typing import AsyncGenerator,Optional
from contextlib import asynccontextmanager
from app import settings
from aiokafka import AIOKafkaProducer
import json


class Order(SQLModel):
    id: Optional[int] = Field(default=None)
    username: str
    product_price: float
    product_id: int
    product_name: str


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    print("Call Kafka Consumer")
    yield


app = FastAPI(
    lifespan=lifespan,
    title="Hello world service with Kafka messaging",
    version="0.1.0",
    server=[{"url": "http://localhost:8000", "description": "Development server"}],
)


@app.get("/")
async def read_root():
    return {"Hello": "World"}


@app.post("/createorder")
async def create_order(order: Order):
    producer = AIOKafkaProducer(bootstrap_servers=settings.BOOTSTRAP_SERVER)
    await producer.start()
    orderJson = json.dumps(order.__dict__).encode("utf-8")
    print("OrderJSON", orderJson)
    try:
        await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC, orderJson)
    finally:
        await producer.stop()

    return orderJson
