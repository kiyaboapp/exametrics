# app/api/v1/exams.py
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.database import get_db
from app.schemas.exam import ExamCreate, Exam
from app.services.exam_service import create_exam, get_exam, get_exams

router = APIRouter()

@router.post("/", response_model=Exam)
async def create_new_exam(exam: ExamCreate, db: AsyncSession = Depends(get_db)):
    return await create_exam(db, exam)

@router.get("/{exam_id}", response_model=Exam)
async def get_exam_by_id(exam_id: str, db: AsyncSession = Depends(get_db)):
    return await get_exam(db, exam_id)

@router.get("/", response_model=list[Exam])
async def get_all_exams(db: AsyncSession = Depends(get_db), skip: int = 0, limit: int = 100):
    return await get_exams(db, skip, limit)