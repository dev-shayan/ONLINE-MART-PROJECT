from sqlmodel import SQLModel, Field
from typing import Optional
from pydantic import BaseModel


class Product(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(index=True, min_length=1, max_length=100)
    description: str = Field(index=True, min_length=1, max_length=500)
    price: float = Field(default=0.0)
    stock: int = Field(default=0)

class ProductUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    price: Optional[float] = None
    stock: Optional[int] = None

