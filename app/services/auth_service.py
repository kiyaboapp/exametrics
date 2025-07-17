from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi import Depends, HTTPException, status
from datetime import datetime
from app.db.models.user import User
from app.core.security import verify_password, get_password_hash, create_access_token
from app.schemas.user import UserCreate, UserInDB
from app.db.database import get_db

async def create_user(db: AsyncSession, user: UserCreate) -> UserInDB:
    result = await db.execute(select(User).filter(User.username == user.username))
    if result.scalars().first():
        raise HTTPException(status_code=400, detail="Username already registered")
    hashed_password = get_password_hash(user.password)
    db_user = User(
        username=user.username,
        email=user.email,
        first_name=user.first_name,
        middle_name=user.middle_name,
        surname=user.surname,
        school_name=user.school_name,
        centre_number=user.centre_number,
        role=user.role,
        hashed_password=hashed_password
    )
    db.add(db_user)
    await db.commit()
    await db.refresh(db_user)
    return UserInDB.from_orm(db_user)

async def authenticate_user(db: AsyncSession, username: str, password: str) -> UserInDB:
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
    return UserInDB.from_orm(user)