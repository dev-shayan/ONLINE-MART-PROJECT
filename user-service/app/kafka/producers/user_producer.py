import logging
from app.protobuf.user_proto import user_pb2
from aiokafka import AIOKafkaProducer
from app import settings

logger = logging.getLogger(__name__)

# Produce Kafka messages for the user service
async def produce_message(user, producer: AIOKafkaProducer, operation: str):
    """
    Produces a message to a Kafka topic.

    Args:
        user: The user object containing user information.
        producer: The AIOKafkaProducer instance used for producing messages.
        operation: The operation to be performed on the user.

    Raises:
        RuntimeError: If there is an error while producing the message.

    Returns:
        None
    """

    try:
        protobuf_user = user_pb2.User(
            user_id=user.user_id,
            full_name=user.full_name,
            email=user.email,
            password=user.password,
            address=user.address,
        )
        logger.info(f"Value of ID: {user.user_id}")
        serialized_user = protobuf_user.SerializeToString()

        operation_bytes = operation.encode("utf-8")  # Convert operation to bytes
        logger.info(f"operation_bytes: {operation_bytes}/")

        await producer.send_and_wait(
            topic=settings.KAFKA_USER_TOPIC,
            value=serialized_user,
            key=operation_bytes,
        )
        logger.info(f"Message produced successfully: {protobuf_user}")
        logger.info(f"Operation: {operation}")
    except Exception as e:
        logger.error(f"Failed to produce message: {str(e)}")
        raise RuntimeError(f"Failed to produce message: {str(e)}")
