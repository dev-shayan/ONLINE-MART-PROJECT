from sqlmodel import Session, select
from sqlalchemy.exc import IntegrityError, OperationalError
from app.models import Product, ProductUpdate
import logging

# Setup logging
logger = logging.getLogger(__name__)

def create_product(session: Session, product: Product) -> Product:
    try:
        session.add(product)
        session.commit()
        session.refresh(product)
        return product
    except IntegrityError as e:
        logger.error(f"IntegrityError while creating product: {e}")
        raise e  # Propagate the exception to be handled by the FastAPI layer
    except OperationalError as e:
        logger.error(f"OperationalError while creating product: {e}")
        raise e
    except Exception as e:
        logger.error(f"Unexpected error while creating product: {e}")
        raise e

def get_all_products(session: Session) -> list[Product]:
    try:
        products = session.exec(select(Product).order_by(Product.id)).all()
        return products
    except OperationalError as e:
        logger.error(f"OperationalError while retrieving products: {e}")
        raise e
    except Exception as e:
        logger.error(f"Unexpected error while retrieving products: {e}")
        raise e

def get_single_product(session: Session, product_id: int) -> Product | None:
    try:
        product = session.exec(select(Product).where(Product.id == product_id)).first()
        return product
    except OperationalError as e:
        logger.error(f"OperationalError while retrieving product {product_id}: {e}")
        raise e
    except Exception as e:
        logger.error(f"Unexpected error while retrieving product {product_id}: {e}")
        raise e

def update_product(session: Session, product_id: int, product_data: ProductUpdate) -> Product | None:
    try:
        # Fetch the existing product from the database
        existing_product = session.exec(select(Product).where(Product.id == product_id)).first()
        
        if existing_product:
            # Convert the update data to a dictionary, excluding unset fields
            data = product_data.dict(exclude_unset=True)
            
            # Update only the provided fields, leaving others unchanged
            for key, value in data.items():
                setattr(existing_product, key, value)
            
            # Commit the changes to the database
            session.add(existing_product)
            session.commit()
            session.refresh(existing_product)
            return existing_product
        
        return None  # If the product was not found
    except IntegrityError as e:
        logger.error(f"IntegrityError while updating product {product_id}: {e}")
        raise e
    except OperationalError as e:
        logger.error(f"OperationalError while updating product {product_id}: {e}")
        raise e
    except Exception as e:
        logger.error(f"Unexpected error while updating product {product_id}: {e}")
        raise e


def delete_product(session: Session, product_id: int) -> bool:
    try:
        product = session.exec(select(Product).where(Product.id == product_id)).first()
        if product:
            session.delete(product)
            session.commit()
            return True
        return False
    except OperationalError as e:
        logger.error(f"OperationalError while deleting product {product_id}: {e}")
        raise e
    except Exception as e:
        logger.error(f"Unexpected error while deleting product {product_id}: {e}")
        raise e

def delete_all_products(session: Session) -> None:
    try:
        session.query(Product).delete(synchronize_session=False)
        session.commit()
    except OperationalError as e:
        logger.error(f"OperationalError while deleting all products: {e}")
        raise e
    except Exception as e:
        logger.error(f"Unexpected error while deleting all products: {e}")
        raise e
