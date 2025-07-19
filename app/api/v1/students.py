# app/api/v1/students.py

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.database import get_db
from app.schemas.student import StudentCreate, Student
from app.services.student_service import create_student, get_student, get_students

router = APIRouter()

@router.post("/", response_model=Student)
async def create_new_student(student: StudentCreate, db: AsyncSession = Depends(get_db)):
    return await create_student(db, student)

@router.get("/{student_global_id}", response_model=Student)
async def get_student_by_id(student_global_id: str, db: AsyncSession = Depends(get_db)):
    return await get_student(db, student_global_id)

@router.get("/", response_model=list[Student])
async def get_all_students(db: AsyncSession = Depends(get_db), skip: int = 0, limit: int = 100):
    return await get_students(db, skip, limit)