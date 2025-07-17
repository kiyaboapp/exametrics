from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.database import get_db
from app.schemas.student import StudentCreate, StudentInDB
from app.services.student_service import create_student, get_student

router = APIRouter()

@router.post("/", response_model=StudentInDB)
async def create_new_student(student: StudentCreate, db: AsyncSession = Depends(get_db)):
    return await create_student(db, student)

@router.get("/{student_global_id}", response_model=StudentInDB)
async def get_student_by_id(student_global_id: str, db: AsyncSession = Depends(get_db)):
    return await get_student(db, student_global_id)