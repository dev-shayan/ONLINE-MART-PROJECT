from fastapi import HTTPException
from sqlmodel import Session, select, asc
import logging
from app.models.order_model import OrderModel, OrderUpdate

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add a new Order
def add_order(order_data, session: Session) -> OrderModel:
    try:
        session.add(order_data)
        session.commit()
        session.refresh(order_data)
        return order_data
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=str(e))


# Get all Orders
def get_all_orders(session: Session) -> list[OrderModel]:
    all_orders = session.exec(select(OrderModel).order_by(asc(OrderModel.id)))
    if all_orders is None:
        raise HTTPException(status_code=404, detail="No Order Found")
    return all_orders


# Get Order by id
def get_order_by_id(id: int, session: Session) -> OrderModel:
    order = session.exec(select(OrderModel).where(OrderModel.id == id)).one_or_none()
    if order is None:
        raise HTTPException(
            status_code=404, detail=f"No Order found with the id : {id}"
        )
    return order


# Update Order by id
def update_order(
    id: int, to_update_order_data: OrderUpdate, session: Session) -> OrderModel:
    # 1. Get the existing order
    order = get_order_by_id(id, session)

    # 2. Update only the fields that are provided
    update_data = to_update_order_data.model_dump(exclude_unset=True)

    for key, value in update_data.items():
        setattr(order, key, value)

    session.add(order)
    session.commit()
    session.refresh(order)
    return order


# Delete Order by id
def delete_order_by_id(id: int, session: Session) -> dict:

    # 1. Get the Order
    order = get_order_by_id(id, session)

    # 2. Delete the Order
    session.delete(order)
    session.commit()

    logging.info(f'''Order with ID {id} deleted and committed to the database. 
    ''')
    return {"message": "Order Deleted Successfully"}


# Check if order exist or not
def validate_id(id: int, session: Session) -> OrderModel | None:
    order = session.exec(select(OrderModel).where(OrderModel.id == id)).one_or_none()
    if not order:
        return None
    return order
