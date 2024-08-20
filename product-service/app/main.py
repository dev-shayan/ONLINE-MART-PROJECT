import logging
from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from sqlmodel import Session
import asyncio
from app import settings
from sqlalchemy.exc import IntegrityError
from pydantic import ValidationError
from aiokafka import AIOKafkaProducer
from app.deps import get_kafka_producer
import json
from app.kafka.producer.productProducer import produce_message
from app.kafka.consumer.productConsumer import consume_products
from typing import Annotated
from contextlib import asynccontextmanager
from app.models import Product, ProductUpdate
from app.database import create_tables
from app.crud.crud import create_product, get_all_products, get_single_product, update_product, delete_product, delete_all_products

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Product Service Starting...")
    
    create_tables()
    task = asyncio.create_task(consume_products(
        settings.KAFKA_PRODUCT_TOPIC, 
        settings.BOOTSTRAP_SERVER, 
        settings.KAFKA_CONSUMER_GROUP_ID_FOR_PRODUCT
        ))
    yield
    logger.info("Product Service Closing...")

app = FastAPI(
    lifespan=lifespan,
    title="Product Catalog Service",
    version="1.0.0",
)

# Custom exception handlers
@app.exception_handler(IntegrityError)
async def integrity_error_handler(request: Request, exc: IntegrityError):
    logger.error(f"IntegrityError: {exc}")
    return JSONResponse(
        status_code=400,
        content={"detail": "A database integrity error occurred."},
    )

@app.exception_handler(ValidationError)
async def validation_error_handler(request: Request, exc: ValidationError):
    logger.error(f"ValidationError: {exc}")
    return JSONResponse(
        status_code=422,
        content={"detail": "Validation error occurred."},
    )

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled Exception: {exc}")
    return JSONResponse(
        status_code=500,
        content={"detail": "An internal server error occurred."},
    )

@app.get("/")
async def root():
    return {"msg": "Welcome to the Product Catalog Service"}

@app.post("/products/", response_model=Product)
async def create_product_endpoint(product: Product, producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]):
    logger.info(f"Creating product: {product}")
    await produce_message(product,producer)
    logger.info(f"Product created: {product}")
    return product


# @app.get("/products/", response_model=list[Product])
# async def get_all_products_endpoint(session: Annotated[Session, Depends(get_session)]):
#     products = get_all_products(session)
#     if products:
#         return products
#     else:
#         raise HTTPException(status_code=404, detail="No products found")

# @app.get("/products/{id}", response_model=Product)
# async def get_single_product_endpoint(id: int, session: Annotated[Session, Depends(get_session)]):
#     product = get_single_product(session, id)
#     if product:
#         return product
#     else:
#         raise HTTPException(status_code=404, detail="Product not found")

# @app.put("/products/{id}")
# async def update_product_endpoint(
#     id: int, product: ProductUpdate, session: Annotated[Session, Depends(get_session)]
# ):
#     updated_product = update_product(session, id, product)
#     if updated_product:
#         return updated_product
#     else:
#         raise HTTPException(status_code=404, detail="Product not found")

# @app.delete("/products/{id}")
# async def delete_product_endpoint(id: int, session: Annotated[Session, Depends(get_session)]):
#     if delete_product(session, id):
#         return {"message": "Product successfully deleted"}
#     else:
#         raise HTTPException(status_code=404, detail="Product not found")

# @app.delete("/products/")
# async def delete_all_products_endpoint(session: Annotated[Session, Depends(get_session)]):
#     delete_all_products(session)
#     return {"message": "All products successfully deleted"}
