
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.api.deps import get_current_user, get_db
from app.db.models.user import User
from app.db.schemas.student import StudentCreate, Student
from app.services.student_service import create_student, get_student, get_students
from typing import List

router = APIRouter(prefix="/students", tags=["students"])

@router.post("/", response_model=Student)
async def create_student_endpoint(student: StudentCreate, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await create_student(db, student)

@router.get("/{student_global_id}", response_model=Student)
async def get_student_endpoint(student_global_id: str, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await get_student(db, student_global_id)

@router.get("/", response_model=List[Student])
async def get_students_endpoint(skip: int = 0, limit: int = 100, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await get_students(db, skip, limit)
