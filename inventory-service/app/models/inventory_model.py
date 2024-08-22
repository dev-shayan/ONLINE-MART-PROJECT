from enum import Enum
from sqlmodel import SQLModel, Field

class InventoryStatus(str, Enum):
    IN_STOCK = "in_stock"
    OUT_OF_STOCK = "out_of_stock"

class Inventory(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    product_id: int
    quantity: int
    unit_price: float
    status: InventoryStatus

class InventoryUpdate(SQLModel):
    quantity: int | None = None
    status: InventoryStatus | None = None