import logging
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError, KafkaError
from fastapi import HTTPException
from app.models.order_model import OrderModel, OrderUpdate
from app.crud.order_crud import add_order, update_order, delete_order_by_id
from app.deps import get_session
from app.protobuf.order_proto import order_pb2
import asyncio
from app.kafka.producers.afterdb_order_producer import produce_afterdb_message


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_RETRIES = 5
RETRY_INTERVAL = 10  # seconds


async def process_message(protobuf_order: order_pb2.Order, operation: str):
    try:
        sqlmodel_order = OrderModel(
            id=protobuf_order.id,
            user_id=protobuf_order.user_id,
            user_email=protobuf_order.user_email,
            user_full_name=protobuf_order.user_full_name,
            user_address=protobuf_order.user_address,
            product_id=protobuf_order.product_id,
            quantity=protobuf_order.quantity,
            total_amount=protobuf_order.total_amount,
            product_title=protobuf_order.product_title,
            product_description=protobuf_order.product_description,
            product_category=protobuf_order.product_category,
            product_brand=protobuf_order.product_brand,
            status=protobuf_order.status,
        )

        if protobuf_order.id == 0:
            sqlmodel_order.id = None

        logger.info(f'''Converted SQLModel Order Data: {sqlmodel_order}''')

        if operation == "create":
            with next(get_session()) as session:
                db_insert_order = add_order(sqlmodel_order, session=session)
                logger.info(f'''DB Inserted Order ID: {db_insert_order.id}''')
                logger.info(f"DB Inserted Order: {db_insert_order}")
                logger.info(f'''
            
Added order in the database
            
''')
            await produce_afterdb_message(sqlmodel_order, operation)

        elif operation == "delete":

            if sqlmodel_order.id is None:
                sqlmodel_order.id = 0
        with next(get_session()) as session:
            sqlmodel_order.status = "cancelled"
            db_update_order = update_order(
            sqlmodel_order.id,
            OrderUpdate(**sqlmodel_order.dict()),
            session=session,
        )
        logger.info(f'''DB Updated Order: {db_update_order}''')
        logger.info(f'''
            
Updated order in the database
            
''')
        await produce_afterdb_message(sqlmodel_order, operation)

    #     elif operation == "delete":
    #         if sqlmodel_order.id is None:
    #             sqlmodel_order.id = 0
    #         logger.info(f"Attempting to delete order with ID: {sqlmodel_order.id}")
    #         db_delete_order = delete_order_by_id(
    #             sqlmodel_order.id, session=session
    #         )
    #         logger.info(f'''DB Deleted Order: {db_delete_order}''')
    #         logger.info(f'''
                
    # Deleted order from the database
                
    # ''')

    except HTTPException as e:
        logger.error(f"HTTPException: {e.detail}")
        raise
    except Exception as e:
        logger.error(f"Exception: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


async def consume_orders(topic, bootstrap_servers, group_id):
    retries = 0

    while retries < MAX_RETRIES:
        try:
            consumer = AIOKafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
                auto_offset_reset="earliest",
            )
            logger.info(f'''

    Consumer created, attempting to start...
    
    ''')
            await consumer.start()
            logger.info(f'''

    Consumer started successfully
            
    ''')
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
            logger.info(f"Received message from topic: {msg.topic}")
            logger.info(f"Message Value: {msg.value}")
            logger.info(f"Message key: {msg.key}")

            protobuf_order = order_pb2.Order()
            protobuf_order.ParseFromString(msg.value)
            logger.info(f'''
        
Value of Order ID from protobuf: {protobuf_order.id}''')
            logger.info(f"Consumed Order Data: {protobuf_order}")
            operation = msg.key.decode("utf-8")  # Decode the operation key
            logger.info(f'''Operation: {operation}
            ''')

            await process_message(protobuf_order, operation)
    except KafkaError as e:
        logger.error(f"Error while consuming message: {e}")
    finally:
        logger.info("Stopping consumer")
        await consumer.stop()
