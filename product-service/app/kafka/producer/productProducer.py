import json
from aiokafka import AIOKafkaProducer
from app.models import Product
from app import settings
import logging

logger = logging.getLogger(__name__)

async def produce_message(product: Product, producer: AIOKafkaProducer):
    logger.info(f"Producing message for product {product.id}")
    product_dict = {field: getattr(product, field) for field in product.dict()}
    product_json = json.dumps(product_dict).encode("utf-8")
    logger.info(f"Producing message: {product_json}")
    
    # Produce message
    await producer.send_and_wait(settings.KAFKA_PRODUCT_TOPIC, product_json)
    logger.info(f"Message produced for product {product.id}")
