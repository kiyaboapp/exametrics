
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.api.deps import get_current_user, get_db
from app.db.models.user import User
from app.db.schemas.region import RegionCreate, Region
from app.services.region_service import create_region, get_region, get_regions
from typing import List

router = APIRouter(prefix="/regions", tags=["regions"])

@router.post("/", response_model=Region)
async def create_region_endpoint(region: RegionCreate, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await create_region(db, region)

@router.get("/{region_id}", response_model=Region)
async def get_region_endpoint(region_id: int, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await get_region(db, region_id)

@router.get("/", response_model=List[Region])
async def get_regions_endpoint(skip: int = 0, limit: int = 100, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await get_regions(db, skip, limit)
