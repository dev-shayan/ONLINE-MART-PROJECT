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
from app.models.user_model import User, UserUpdate
from app.crud.user_crud import (
    get_all_users,
    get_user_by_id,
    validate_id,
    validate_email,
    update_user,
)
from app.kafka.producers.user_producer import produce_message
from app.kafka.consumers.user_consumer import consume_users

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_db_and_tables() -> None:
    """
    Create the database tables.

    This function creates the necessary tables in the database using the SQLModel metadata and the engine.

    Args:
        None

    Returns:
        None
    """
    SQLModel.metadata.create_all(engine)
    logger.info(f'''
    
    Database tables created successfully
    
    ''')

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Context manager for the lifespan of the FastAPI application.

    This context manager is responsible for starting and closing the User Service.
    It creates the database tables, starts the Kafka consumer, and logs the start and end of the service.

    Args:
        app (FastAPI): The FastAPI application instance.

    Yields:
        None

    Returns:
        None
    """
    logger.info(f'''

    User Service Starting...
    
    ''')
    create_db_and_tables()
    task = asyncio.create_task(
        consume_users(
            settings.KAFKA_USER_TOPIC,
            settings.BOOTSTRAP_SERVER,
            settings.KAFKA_CONSUMER_GROUP_ID_FOR_USER,
        )
    )
    yield
    logger.info("User Service Closing...")

app = FastAPI(
    lifespan=lifespan,
    title="User Service",
    version="0.0.1",
)

@app.get("/")
def start():
    """
    Default route handler.

    This function returns a simple message indicating that the User Service is running.

    Args:
        None

    Returns:
        dict: A dictionary containing the message.
    """
    return {"message": "User Service"}

@app.post("/user", response_model=User)
async def call_add_user(
    user: User,
    session: Annotated[Session, Depends(get_session)],
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)],
):
    """
    Add a new user.

    This function adds a new user to the database.
    It validates if the user ID or email already exists and raises an exception if it does.
    It then produces a Kafka message to create the user.

    Args:
        user (User): The user data.
        session (Session): The database session.
        producer (AIOKafkaProducer): The Kafka producer.

    Returns:
        User: The created user.
    """
    # existing_user = validate_id(user.user_id, session)
    existing_email = validate_email(user.email, session)


    # if existing_user or existing_email:
    #     raise HTTPException(
    #         status_code=400, 
    #         detail=f"User with ID {user.user_id} or Email {user.email} already exists"
    #     )
    if existing_email:
        raise HTTPException(
            status_code=400, 
            detail=f"User with Email {user.email} already exists"
        )

    await produce_message(user, producer, "create")

    return user


@app.get("/user/all", response_model=list[User])
def call_get_all_users(session: Annotated[Session, Depends(get_session)]):
    """
    Get all users.

    This function retrieves all users from the database.

    Args:
        session (Session): The database session.

    Returns:
        list[User]: A list of all users.
    """
    return get_all_users(session)

@app.get("/user/{id}", response_model=User)
def call_get_user_by_id(id: int, session: Annotated[Session, Depends(get_session)]):
    """
    Get a user by ID.

    This function retrieves a user from the database based on the provided ID.

    Args:
        id (int): The ID of the user.
        session (Session): The database session.

    Returns:
        User: The user with the specified ID.
    """
    return get_user_by_id(id=id, session=session)

@app.patch("/user/{id}", response_model=User)
async def call_update_user(
    id: int,
    user: UserUpdate,
    session: Annotated[Session, Depends(get_session)],
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)],
):
    """
    Update a user.

    This function updates a user in the database.
    It retrieves the existing user, updates the provided fields, and produces a Kafka message to update the user.

    Args:
        id (int): The ID of the user.
        user (UserUpdate): The updated user data.
        session (Session): The database session.
        producer (AIOKafkaProducer): The Kafka producer.

    Returns:
        User: The updated user.
    """
    logger.info(f'''User id {id} User Update: {user}
    
    ''')

    # Get the existing user
    existing_user = get_user_by_id(id, session)

    # Update the existing user only with the provided fields
    updated_user = update_user(id, user, session)

    logger.info(f'''Updated User: {updated_user}
    
    ''')

    # Produce the Kafka message
    await produce_message(updated_user, producer, "update")

    return updated_user

@app.delete("/user/{id}", response_model=dict)
async def call_delete_user_by_id(
    id: int,
    session: Annotated[Session, Depends(get_session)],
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)],
):
    """
    Delete a user by ID.

    This function deletes a user from the database based on the provided ID.
    It retrieves the user, produces a Kafka message to delete the user, and returns the deleted ID.

    Args:
        id (int): The ID of the user.
        session (Session): The database session.
        producer (AIOKafkaProducer): The Kafka producer.

    Returns:
        dict: A dictionary containing the deleted ID.
    """
    call_get_user_by_id(id, session)
    await produce_message(User(user_id=id), producer, "delete")

    return {"deleted_id": id}
