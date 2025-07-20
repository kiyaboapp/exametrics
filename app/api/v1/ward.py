
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.api.deps import get_current_user, get_db
from app.db.models.user import User
from app.db.schemas.ward import WardCreate, Ward
from app.services.ward_service import create_ward, get_ward, get_wards
from typing import List

router = APIRouter(prefix="/wards", tags=["wards"])

@router.post("/", response_model=Ward)
async def create_ward_endpoint(ward: WardCreate, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await create_ward(db, ward)

@router.get("/{ward_id}", response_model=Ward)
async def get_ward_endpoint(ward_id: int, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await get_ward(db, ward_id)

@router.get("/", response_model=List[Ward])
async def get_wards_endpoint(skip: int = 0, limit: int = 100, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await get_wards(db, skip, limit)
