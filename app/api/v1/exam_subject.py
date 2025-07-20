
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.api.deps import get_current_user, get_db
from app.db.models.user import User
from app.db.schemas.exam_subject import ExamSubjectCreate, ExamSubject
from app.services.exam_subject_service import create_exam_subject, get_exam_subject, get_exam_subjects
from typing import List

router = APIRouter(prefix="/exam-subjects", tags=["exam-subjects"])

@router.post("/", response_model=ExamSubject)
async def create_exam_subject_endpoint(exam_subject: ExamSubjectCreate, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await create_exam_subject(db, exam_subject)

@router.get("/{exam_subject_id}", response_model=ExamSubject)
async def get_exam_subject_endpoint(exam_subject_id: int, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await get_exam_subject(db, exam_subject_id)

@router.get("/", response_model=List[ExamSubject])
async def get_exam_subjects_endpoint(skip: int = 0, limit: int = 100, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await get_exam_subjects(db, skip, limit)
