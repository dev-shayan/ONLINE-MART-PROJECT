import logging
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError, KafkaError
from fastapi import HTTPException
from app.models.user_model import User, UserUpdate
from app.crud.user_crud import add_user, update_user, delete_user_by_id
from app.deps import get_session
from app.protobuf.user_proto import user_pb2
import asyncio

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_RETRIES = 5
RETRY_INTERVAL = 10  # seconds

async def process_message(protobuf_user: user_pb2.User, operation: str):
    """
    Process a message received from Kafka.

    Args:
        protobuf_user (user_pb2.User): The user data received from Kafka as a protobuf message.
        operation (str): The operation to perform on the user data (create, update, delete).

    Raises:
        HTTPException: If there is an HTTP exception during processing.
        Exception: If there is an unexpected exception during processing.

    Returns:
        None

    """
    try:
        sqlmodel_user = User(
            user_id=protobuf_user.user_id,
            full_name=protobuf_user.full_name,
            email=protobuf_user.email,
            password=protobuf_user.password,
            address=protobuf_user.address,
        )
        # Only set the id if it's not 0
        if protobuf_user.user_id == 0:
            sqlmodel_user.user_id = None

        logger.info(f'''Converted SQLModel User Data: {sqlmodel_user}
        
        ''')

        with next(get_session()) as session:
            if operation == "create":
                db_insert_user = add_user(sqlmodel_user, session=session)
                logger.info(f'''DB Inserted User ID: {db_insert_user.user_id}
                
                ''')
                logger.info(f"DB Inserted User: {db_insert_user}")
                logger.info(f'''Added user to the database'''
                
                )

            elif operation == "update":
                if sqlmodel_user.user_id is None:
                    sqlmodel_user.user_id = 0
                db_update_user = update_user(
                    sqlmodel_user.user_id,
                    UserUpdate(**sqlmodel_user.dict()),
                    session=session,
                )
                logger.info(f'''DB Updated User: {db_update_user}
                ''')
                logger.info(f'''
                
    Updated user in the database
                
    ''')

            elif operation == "delete":
                if sqlmodel_user.user_id is None:
                    sqlmodel_user.user_id = 0
                logger.info(f"Attempting to delete user with ID: {sqlmodel_user.user_id}")
                db_delete_user = delete_user_by_id(
                    sqlmodel_user.user_id, session=session
                )
                logger.info(f'''DB Deleted User: {db_delete_user}
                ''')
                logger.info(f'''
                
    Deleted user from the database
                
    ''')

    except HTTPException as e:
        logger.error(f"HTTPException: {e.detail}")
        raise
    except Exception as e:
        logger.error(f"Exception: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

async def consume_users(topic, bootstrap_servers, group_id):
    """
    Consume user messages from Kafka.

    Args:
        topic (str): The Kafka topic to consume from.
        bootstrap_servers (str): The Kafka bootstrap servers.
        group_id (str): The consumer group ID.

    """
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
    
    '''
            )
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

            protobuf_user = user_pb2.User()
            protobuf_user.ParseFromString(msg.value)
            logger.info(f'''
        
Value of User ID from protobuf: {protobuf_user.user_id}''')
            logger.info(f"Consumed User Data: {protobuf_user}")
            operation = msg.key.decode("utf-8")  # Decode the operation key
            logger.info(f'''Operation: {operation}
            ''')

            await process_message(protobuf_user, operation)
    except KafkaError as e:
        logger.error(f"Error while consuming message: {e}")
    finally:
        logger.info("Stopping consumer")
        await consumer.stop()
