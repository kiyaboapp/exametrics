
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.api.deps import get_current_user, get_db
from app.db.models.user import User
from app.db.schemas.user import UserCreate, User as UserSchema
from app.services.user_service import create_user, get_user, get_users
from typing import List

router = APIRouter(prefix="/users", tags=["users"])

@router.post("/", response_model=UserSchema)
async def create_user_endpoint(user: UserCreate, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await create_user(db, user)

@router.get("/{user_id}", response_model=UserSchema)
async def get_user_endpoint(user_id: str, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await get_user(db, user_id)

@router.get("/", response_model=List[UserSchema])
async def get_users_endpoint(skip: int = 0, limit: int = 100, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await get_users(db, skip, limit)
