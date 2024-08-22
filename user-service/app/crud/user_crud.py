from fastapi import HTTPException
from sqlmodel import Session, select, asc
import logging
from app.models.user_model import User, UserUpdate

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def add_user(user_data, session: Session) -> User:
    """
    Add a new user to the database.

    Args:
        user_data: Data of the user to be added.
        session: Database session.

    Returns:
        The added user.

    Raises:
        HTTPException: If there is an error adding the user.
    """
    try:
        if user_data.user_id is not None:
            session.add(user_data)
            session.commit()
            session.refresh(user_data)
            return user_data
        else:
            # No ID provided, let the database handle auto-increment
            session.add(user_data)
            session.commit()
            session.refresh(user_data)
            return user_data
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=str(e))

def get_all_users(session: Session) -> list[User]:
    """
    Get all users from the database.

    Args:
        session: Database session.

    Returns:
        A list of all users.

    Raises:
        HTTPException: If no users are found.
    """
    all_users = session.exec(select(User).order_by(asc(User.user_id)))
    if all_users is None:
        raise HTTPException(status_code=404, detail="No User Found")
    return all_users

def get_user_by_id(id: int, session: Session) -> User:
    """
    Get a user by their ID from the database.

    Args:
        id: ID of the user.
        session: Database session.

    Returns:
        The user with the specified ID.

    Raises:
        HTTPException: If no user is found with the specified ID.
    """
    user = session.exec(select(User).where(User.user_id == id)).one_or_none()
    if user is None:
        raise HTTPException(
            status_code=404, detail=f"No User found with the id : {id}"
        )
    return user

def update_user(
    id: int, to_update_user_data: UserUpdate, session: Session) -> User:
    """
    Update a user in the database.

    Args:
        id: ID of the user to be updated.
        to_update_user_data: Data to update the user with.
        session: Database session.

    Returns:
        The updated user.

    Raises:
        HTTPException: If no user is found with the specified ID.
    """
    # 1. Get the existing user
    user = get_user_by_id(id, session)

    # 2. Update only the fields that are provided
    update_data = to_update_user_data.model_dump(exclude_unset=True)

    for key, value in update_data.items():
        setattr(user, key, value)

    session.add(user)
    session.commit()
    session.refresh(user)
    return user

def delete_user_by_id(id: int, session: Session) -> dict:
    """
    Delete a user from the database.

    Args:
        id: ID of the user to be deleted.
        session: Database session.

    Returns:
        A dictionary with a success message.

    Raises:
        HTTPException: If no user is found with the specified ID.
    """
    # 1. Get the User
    user = get_user_by_id(id, session)

    # 2. Delete the User
    session.delete(user)
    session.commit()

    logging.info(f'''User with ID {id} deleted and committed to the database. 
    ''')
    return {"message": "User Deleted Successfully"}

def validate_id(id: int, session: Session) -> User | None:
    """
    Validate if a user exists in the database.

    Args:
        id: ID of the user to validate.
        session: Database session.

    Returns:
        The user if it exists, otherwise None.
    """
    user = session.exec(select(User).where(User.user_id == id)).one_or_none()
    if not user:
        return None
    return user

def validate_email(email: str, session: Session) -> User | None:
    """
    Get a user by their email from the database.

    Args:
        email: Email of the user.
        session: Database session.

    Returns:
        The user with the specified email if it exists, otherwise None.
    """
    user = session.exec(select(User).where(User.email == email)).one_or_none()
    if not user:
        return None
    return user
