import logging
from app.protobuf.order_proto import order_pb2
from aiokafka import AIOKafkaProducer
from app import settings

logger = logging.getLogger(__name__)

async def produce_message(order, producer: AIOKafkaProducer, operation: str):

    try:
        protobuf_order = order_pb2.Order(
            id=order.id,
            user_id=order.user_id,
            user_email=order.user_email,
            user_full_name=order.user_full_name,
            user_address=order.user_address,
            product_id=order.product_id,
            quantity=order.quantity,
            total_amount=order.total_amount,
            product_title=order.product_title,
            product_description=order.product_description,
            product_category=order.product_category,
            product_brand=order.product_brand,
            status=order.status,
        )
        logger.info(f"Value of ID: {order.id}")
        serialized_order = protobuf_order.SerializeToString()

        operation_bytes = operation.encode("utf-8")  # Convert operation to bytes
        logger.info(f"operation_bytes: {operation_bytes}/")

        await producer.send_and_wait(
            topic=settings.KAFKA_ORDER_TOPIC,
            value=serialized_order,
            key=operation_bytes,
        )
        logger.info(f"Message produced successfully: {protobuf_order}")
        logger.info(f"Operation: {operation}")
    except Exception as e:
        logger.error(f"Failed to produce message: {str(e)}")
        raise RuntimeError(f"Failed to produce message: {str(e)}")
