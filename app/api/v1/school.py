
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.api.deps import get_current_user, get_db
from app.db.models.user import User
from app.db.schemas.school import SchoolCreate, School
from app.services.school_service import create_school, get_school, get_schools
from typing import List

router = APIRouter(prefix="/schools", tags=["schools"])

@router.post("/", response_model=School)
async def create_school_endpoint(school: SchoolCreate, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await create_school(db, school)

@router.get("/{centre_number}", response_model=School)
async def get_school_endpoint(centre_number: str, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await get_school(db, centre_number)

@router.get("/", response_model=List[School])
async def get_schools_endpoint(skip: int = 0, limit: int = 100, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await get_schools(db, skip, limit)
