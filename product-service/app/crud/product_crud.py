from fastapi import HTTPException
from sqlmodel import Session, select, asc
import logging
from app.models.product_model import Product, ProductUpdate

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add a new Product
def add_product(product_data, session: Session) -> Product:
    try:
        if product_data.id is not None:
            session.add(product_data)
            session.commit()
            session.refresh(product_data)
            return product_data
        else:
            # No ID provided, let the database handle auto-increment
            session.add(product_data)
            session.commit()
            session.refresh(product_data)
            return product_data
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=str(e))


# Get all Products
def get_all_products(session: Session) -> list[Product]:
    all_products = session.exec(select(Product).order_by(asc(Product.id)))
    if all_products is None:
        raise HTTPException(status_code=404, detail="No Product Found")
    return all_products


# Get Product by id
def get_product_by_id(id: int, session: Session) -> Product:
    product = session.exec(select(Product).where(Product.id == id)).one_or_none()
    if product is None:
        raise HTTPException(
            status_code=404, detail=f"No Product found with the id : {id}"
        )
    return product


# Update Product by id
def update_product(
    id: int, to_update_product_data: ProductUpdate, session: Session) -> Product:
    # 1. Get the existing product
    product = get_product_by_id(id, session)

    # 2. Update only the fields that are provided
    update_data = to_update_product_data.model_dump(exclude_unset=True)

    for key, value in update_data.items():
        setattr(product, key, value)

    session.add(product)
    session.commit()
    session.refresh(product)
    return product


# Delete Product by id
def delete_product_by_id(id: int, session: Session) -> dict:

    # 1. Get the Product
    product = get_product_by_id(id, session)

    # 2. Delete the Product
    session.delete(product)
    session.commit()

    logging.info(f"Product with ID {id} deleted and committed to the database.")
    return {"message": "Product Deleted Successfully"}


# Check if product exist or not
def validate_id(id: int, session: Session) -> Product | None:
    product = session.exec(select(Product).where(Product.id == id)).one_or_none()
    if not product:
        return None
    return product
