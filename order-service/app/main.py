import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException
from sqlmodel import Session, SQLModel
from typing import Annotated, AsyncGenerator
from aiokafka import AIOKafkaProducer
import asyncio

from app import settings
from app.db_engine import engine
from app.deps import get_session, kafka_producer
from app.models.order_model import OrderModel, OrderUpdate
from app.crud.order_crud import (
    get_all_orders,
    get_order_by_id,
    validate_id,
    update_order,
)
from app.kafka.producers.order_producer import produce_message
from app.kafka.consumers.order_consumer import consume_orders

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_db_and_tables() -> None:
    SQLModel.metadata.create_all(engine)
    logger.info(f'''
    
    Database tables created successfully
    
    ''')

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    logger.info(f'''

    Order Service Starting...
    
    ''')
    create_db_and_tables()
    task = asyncio.create_task(
        consume_orders(
            settings.KAFKA_ORDER_TOPIC,
            settings.BOOTSTRAP_SERVER,
            settings.KAFKA_CONSUMER_GROUP_ID_FOR_ORDER,
        )
    )
    yield
    logger.info("Order Service Closing...")

app = FastAPI(
    lifespan=lifespan,
    title="Order Service",
    version="0.0.1",
)

@app.get("/")
def start():
    return {"message": "Order Service"}

@app.post("/order", response_model=OrderModel)
async def call_add_order(
    order: OrderModel,
    session: Annotated[Session, Depends(get_session)],
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)],
):

    existing_order = validate_id(order.id, session)

    if existing_order:
        raise HTTPException(
            status_code=400, detail=f'''Order with ID {order.id} already exists
            
            '''
        )

    await produce_message(order, producer, "create")

    return order

@app.get("/order/all", response_model=list[OrderModel])
def call_get_all_orders(session: Annotated[Session, Depends(get_session)]):
    return get_all_orders(session)

@app.get("/order/{id}", response_model=OrderModel)
def call_get_order_by_id(id: int, session: Annotated[Session, Depends(get_session)]):
    return get_order_by_id(id=id, session=session)

@app.patch("/order/{id}", response_model=OrderModel)
async def call_update_order(
    id: int,
    order: OrderUpdate,
    session: Annotated[Session, Depends(get_session)],
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)],
):
    logger.info(f'''Order id {id} Order Update: {order}
    
    ''')

    # Get the existing order
    existing_order = get_order_by_id(id, session)

    # Update the existing order only with the provided fields
    updated_order = update_order(id, order, session)

    logger.info(f'''Updated Order: {updated_order}
    
    ''')

    # Produce the Kafka message
    await produce_message(updated_order, producer, "update")

    return updated_order

@app.delete("/order/{id}", response_model=dict)
async def call_delete_order_by_id(
    id: int,
    session: Annotated[Session, Depends(get_session)],
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)],
):

    call_get_order_by_id(id, session)
    await produce_message(OrderModel(id=id), producer, "delete")

    return {"deleted_id": id}
