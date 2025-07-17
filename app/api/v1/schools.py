from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.database import get_db
from app.schemas.school import SchoolCreate, SchoolInDB
from app.services.school_service import create_school, get_school

router = APIRouter()

@router.post("/", response_model=SchoolInDB)
async def create_new_school(school: SchoolCreate, db: AsyncSession = Depends(get_db)):
    return await create_school(db, school)

@router.get("/{centre_number}", response_model=SchoolInDB)
async def get_school_by_id(centre_number: str, db: AsyncSession = Depends(get_db)):
    return await get_school(db, centre_number)