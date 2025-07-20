from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.db.models.user import User
from app.db.models.school import School
from app.db.schemas.user import UserCreate
from app.core.security import get_password_hash, verify_password
from uuid6 import uuid6
from datetime import datetime

async def create_user(db: AsyncSession, user: UserCreate):
    result = await db.execute(select(User).filter(User.username == user.username))
    if result.scalars().first():
        raise HTTPException(status_code=400, detail="Username already registered")
    
    result = await db.execute(select(School).filter(School.centre_number == user.centre_number))
    if not result.scalars().first():
        raise HTTPException(status_code=400, detail="School not found")
    
    hashed_password = get_password_hash(user.password)
    db_user = User(
        id=str(uuid6()),
        username=user.username,
        email=user.email,
        first_name=user.first_name,
        middle_name=user.middle_name,
        surname=user.surname,
        centre_number=user.centre_number,
        role=user.role,
        hashed_password=hashed_password,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
        is_active=True,
        is_verified=False
    )
    db.add(db_user)
    await db.commit()
    await db.refresh(db_user)
    return db_user

async def authenticate_user(db: AsyncSession, username: str, password: str):
    result = await db.execute(select(User).filter(User.username == username))
    user = result.scalars().first()
    if not user or not verify_password(password, user.hashed_password):
        raise HTTPException(status_code=401, detail="Incorrect username or password")
    return user