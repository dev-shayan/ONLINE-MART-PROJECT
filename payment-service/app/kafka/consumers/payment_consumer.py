import logging
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError, KafkaError
from fastapi import HTTPException
from app.models.payment_model import Payment
from app.crud.payment_crud import create_payment, update_payment, delete_payment
from app.deps import get_session
from app.protobuf.order_proto import order_pb2
import asyncio

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_RETRIES = 5
RETRY_INTERVAL = 10  # seconds


async def process_message(protobuf_payment: order_pb2.Order, operation: str):
    try:
        sqlmodel_payment = Payment(
            order_id=protobuf_payment.id,
            user_id=protobuf_payment.user_id,
            amount=protobuf_payment.total_amount,
            status=protobuf_payment.status,
        )

        if protobuf_payment.id == 0:
            sqlmodel_payment.id = None

        logger.info(f'''Converted SQLModel Payment Data: {sqlmodel_payment}''')

        with next(get_session()) as session:
            if operation == "create":
                db_insert_payment = create_payment(sqlmodel_payment, session=session)
                logger.info(f'''DB Inserted Payment ID: {db_insert_payment.id}''')
                logger.info(f"DB Inserted Payment: {db_insert_payment}")
                logger.info(f'''
                
    Added payment in the database
                
    ''')

    #         elif operation == "update":
    #             if sqlmodel_payment.id is None:
    #                 sqlmodel_payment.id = 0
    #             db_update_payment = update_payment(
    #                 sqlmodel_payment.id,
    #                 PaymentUpdate(**sqlmodel_payment.dict()),
    #                 session=session,
    #             )
    #             logger.info(f'''DB Updated Payment: {db_update_payment}''')
    #             logger.info(f'''
                
    # Updated payment in the database
                
    # ''')

    #         elif operation == "delete":
    #             if sqlmodel_payment.id is None:
    #                 sqlmodel_payment.id = 0
    #             logger.info(f"Attempting to delete payment with ID: {sqlmodel_payment.id}")
    #             db_delete_payment = delete_payment(
    #                 sqlmodel_payment.id, session=session
    #             )
    #             logger.info(f'''DB Deleted Payment: {db_delete_payment}''')
    #             logger.info(f'''
                
    # Deleted payment from the database
                
    # ''')

    except HTTPException as e:
        logger.error(f"HTTPException: {e.detail}")
        raise
    except Exception as e:
        logger.error(f"Exception: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


async def consume_payments(topic, bootstrap_servers, group_id):
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

    Payment Consumer created, attempting to start...
    
    ''')
            await consumer.start()
            logger.info(f'''

    Payment Consumer started successfully
            
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
            logger.info(f"Received message on topic: {msg.topic}")
            logger.info(f"Message Value: {msg.value}")
            logger.info(f"Message key: {msg.key}")

            protobuf_payment = order_pb2.Order()
            protobuf_payment.ParseFromString(msg.value)
            logger.info(f'''
        
Value of Payment ID from protobuf: {protobuf_payment.id}''')
            logger.info(f"Consumed Payment Data: {protobuf_payment}")
            operation = msg.key.decode("utf-8")  # Decode the operation key
            logger.info(f'''Operation: {operation}
            ''')

            await process_message(protobuf_payment, operation)
    except KafkaError as e:
        logger.error(f"Error while consuming message: {e}")
    finally:
        logger.info("Stopping Payment Consumer")
        await consumer.stop()
