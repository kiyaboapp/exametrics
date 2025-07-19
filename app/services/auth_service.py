# app/services/auth_service.py

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi import HTTPException, status
from datetime import datetime
from app.db.models import User, School
from app.core.security import verify_password, get_password_hash, create_access_token
from app.schemas.user import UserCreate, User
import uuid6

async def create_user(db: AsyncSession, user: UserCreate) -> User:
    # Check for existing user
    result = await db.execute(select(User).filter(User.username == user.username))
    if result.scalars().first():
        raise HTTPException(status_code=400, detail="Username already registered")
    
    # Validate centre_number
    if user.centre_number:
        school_exists = await db.execute(select(School).filter(School.centre_number == user.centre_number))
        if not school_exists.scalars().first():
            raise HTTPException(status_code=400, detail="School with provided centre_number does not exist")

    user_data = user.dict(exclude={"password"})
    user_data["hashed_password"] = get_password_hash(user.password)
    user_data["id"] = str(uuid6())
    db_user = User(**user_data)
    db.add(db_user)
    await db.commit()
    await db.refresh(db_user)
    return User.from_orm(db_user)

async def authenticate_user(db: AsyncSession, username: str, password: str) -> User:
    result = await db.execute(select(User).filter(User.username == username))
    user = result.scalars().first()
    if not user or not verify_password(password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    user.last_login = datetime.utcnow()
    await db.commit()
    return User.from_orm(user)