
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi import HTTPException, status
from app.db.models.user import User as UserModel
from app.db.schemas.user import UserCreate, User
from app.core.security import get_password_hash
from uuid6 import uuid6

async def create_user(db: AsyncSession, user: UserCreate) -> User:
    existing_user = await db.execute(select(UserModel).filter((UserModel.username == user.username) | (UserModel.email == user.email)))
    if existing_user.scalars().first():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="User already exists with this username or email"
        )
    user_data = user.model_dump()
    user_data["id"] = str(uuid6())
    user_data["hashed_password"] = get_password_hash(user_data.pop("password"))
    db_user = UserModel(**user_data)
    db.add(db_user)
    await db.commit()
    await db.refresh(db_user)
    return User.model_validate(db_user)

async def get_user(db: AsyncSession, user_id: str) -> User:
    result = await db.execute(select(UserModel).filter(UserModel.id == user_id))
    user = result.scalars().first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return User.model_validate(user)

async def get_users(db: AsyncSession, skip: int = 0, limit: int = 100) -> list[User]:
    result = await db.execute(select(UserModel).offset(skip).limit(limit))
    return [User.model_validate(user) for user in result.scalars().all()]

async def get_user_by_username(db: AsyncSession, username: str) -> User:
    result = await db.execute(select(UserModel).filter(UserModel.username == username))
    user = result.scalars().first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return User.model_validate(user)
