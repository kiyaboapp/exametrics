# app/api/v1/exam_subjects.py

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.database import get_db
from app.schemas.exam_subject import ExamSubjectCreate, ExamSubject
from app.services.exam_subject_service import create_exam_subject, get_exam_subject, get_exam_subjects

router = APIRouter()

@router.post("/", response_model=ExamSubject)
async def create_new_exam_subject(exam_subject: ExamSubjectCreate, db: AsyncSession = Depends(get_db)):
    return await create_exam_subject(db, exam_subject)

@router.get("/{exam_id}/{subject_code}", response_model=ExamSubject)
async def get_exam_subject_by_id(exam_id: str, subject_code: str, db: AsyncSession = Depends(get_db)):
    return await get_exam_subject(db, exam_id, subject_code)

@router.get("/{exam_id}", response_model=list[ExamSubject])
async def get_exam_subjects_by_exam(exam_id: str, db: AsyncSession = Depends(get_db)):
    return await get_exam_subjects(db, exam_id)