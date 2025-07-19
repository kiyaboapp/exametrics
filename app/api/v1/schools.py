# app/api/v1/schools.py

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.database import get_db
from app.schemas.school import SchoolCreate, School
from app.services.school_service import create_school, get_school, get_schools

router = APIRouter()

@router.post("/", response_model=School)
async def create_new_school(school: SchoolCreate, db: AsyncSession = Depends(get_db)):
    return await create_school(db, school)

@router.get("/{centre_number}", response_model=School)
async def get_school_by_id(centre_number: str, db: AsyncSession = Depends(get_db)):
    return await get_school(db, centre_number)

@router.get("/", response_model=list[School])
async def get_all_schools(db: AsyncSession = Depends(get_db), skip: int = 0, limit: int = 100):
    return await get_schools(db, skip, limit)