# app/api/v1/exam_grades.py

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.api.deps import get_current_user, get_db
from app.db.models.user import User
from app.db.schemas.exam_grade import ExamGradeCreate, ExamGrade
from app.services.exam_grade_service import create_exam_grade, get_exam_grade, get_exam_grades
from typing import List

router = APIRouter(prefix="/exam-grades", tags=["exam-grades"])

@router.post("/", response_model=ExamGrade)
async def create_exam_grade_endpoint(exam_grade: ExamGradeCreate, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await create_exam_grade(db, exam_grade)

@router.get("/{exam_grade_id}", response_model=ExamGrade)
async def get_exam_grade_endpoint(exam_grade_id: int, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await get_exam_grade(db, exam_grade_id)

@router.get("/", response_model=List[ExamGrade])
async def get_exam_grades_endpoint(skip: int = 0, limit: int = 100, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await get_exam_grades(db, skip, limit)
