from fastapi import HTTPException
from sqlmodel import Session, select, asc
import logging
from app.models.inventory_model import Inventory, InventoryUpdate

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add a new Inventory entry
def add_inventory(inventory_data, session: Session) -> Inventory:
    try:
        if inventory_data.id is not None:
            session.add(inventory_data)
            session.commit()
            session.refresh(inventory_data)
            return inventory_data
        else:
            # No ID provided, let the database handle auto-increment
            session.add(inventory_data)
            session.commit()
            session.refresh(inventory_data)
            return inventory_data
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=str(e))


# Get all Inventory entries
def get_all_inventory(session: Session) -> list[Inventory]:
    all_inventory = session.exec(select(Inventory).order_by(asc(Inventory.id)))
    if all_inventory is None:
        raise HTTPException(status_code=404, detail="No Inventory Found")
    return all_inventory


# Get Inventory by id
def get_inventory_by_id(id: int, session: Session) -> Inventory:

    logger.info(f'''Getting Inventory with ID {id}''')
    inventory = session.exec(select(Inventory).where(Inventory.id == id)).one_or_none()

    if inventory is None:
        raise HTTPException(
            status_code=404, detail=f"No Inventory found with the id : {id}"
        )
    return inventory

# Get Inventory Item by product_id
def get_inventory_by_product_id(product_id: int, session: Session) -> Inventory | None:
    inventory_item = session.query(Inventory).filter(Inventory.product_id == product_id).first()
    return inventory_item


# Update Inventory by id
def update_inventory(
    id: int, to_update_inventory_data: InventoryUpdate, session: Session) -> Inventory:
    logger.info(f'''Updating Inventory with ID {id} with data: {to_update_inventory_data} ''')
    # 1. Get the existing inventory
    inventory = get_inventory_by_id(id, session)
    logger.info(f'''Existing Inventory: {inventory}''')
    # 2. Update only the fields that are provided
    update_data = to_update_inventory_data.model_dump(exclude_unset=True)

    for key, value in update_data.items():
        setattr(inventory, key, value)

    session.add(inventory)
    session.commit()
    session.refresh(inventory)
    return inventory


# Delete Inventory by id
def delete_inventory_by_id(id: int, session: Session) -> dict:

    # 1. Get the Inventory
    inventory = get_inventory_by_id(id, session)

    # 2. Delete the Inventory
    session.delete(inventory)
    session.commit()

    logging.info(f'''Inventory with ID {id} deleted and committed to the database. 
    ''')
    return {"message": "Inventory Deleted Successfully"}


# Check if inventory entry exists or not
def validate_id(id: int, session: Session) -> Inventory | None:
    inventory = session.exec(select(Inventory).where(Inventory.id == id)).one_or_none()
    if not inventory:
        return None
    return inventory
