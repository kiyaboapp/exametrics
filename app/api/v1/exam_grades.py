# app/api/v1/exam_grades.py
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.database import get_db
from app.schemas.exam_grade import ExamGradeCreate, ExamGrade
from app.services.exam_grade_service import create_exam_grade, get_exam_grade, get_exam_grades

router = APIRouter()

@router.post("/", response_model=ExamGrade)
async def create_new_exam_grade(grade: ExamGradeCreate, db: AsyncSession = Depends(get_db)):
    return await create_exam_grade(db, grade)

@router.get("/{exam_id}/{grade}", response_model=ExamGrade)
async def get_exam_grade_by_id(exam_id: str, grade: str, db: AsyncSession = Depends(get_db)):
    return await get_exam_grade(db, exam_id, grade)

@router.get("/{exam_id}", response_model=list[ExamGrade])
async def get_exam_grades_by_exam(exam_id: str, db: AsyncSession = Depends(get_db)):
    return await get_exam_grades(db, exam_id)