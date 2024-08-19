import logging
from sqlmodel import SQLModel, create_engine, Session
from sqlalchemy.exc import IntegrityError, OperationalError
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from app import settings


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Connection string and database configuration
Connection_string: str = str(settings.DATABASE_URL).replace(
    "postgresql", "postgresql+psycopg"
)

try:
    engine = create_engine(
        Connection_string,
        pool_recycle=300,
        echo=False,
    )
except Exception as e:
    logger.error(f"Failed to initialize database engine, There is something wrong with connection string: {e}")
    raise HTTPException(status_code=500, detail="Database initialization failed")

def create_tables():
    SQLModel.metadata.create_all(engine)


def get_session():
    try:
        with Session(engine) as session:
            yield session
    except OperationalError as e:
        logger.error(f"Database connection failed,something wrong with the session: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")


