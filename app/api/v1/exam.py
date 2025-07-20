
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.api.deps import get_current_user, get_db
from app.db.models.user import User
from app.db.schemas.exam import ExamCreate, Exam
from app.services.exam_service import create_exam, get_exam, get_exams
from typing import List

router = APIRouter(prefix="/exams", tags=["exams"])

@router.post("/", response_model=Exam)
async def create_exam_endpoint(exam: ExamCreate, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await create_exam(db, exam)

@router.get("/{exam_id}", response_model=Exam)
async def get_exam_endpoint(exam_id: str, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await get_exam(db, exam_id)

@router.get("/", response_model=List[Exam])
async def get_exams_endpoint(skip: int = 0, limit: int = 100, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await get_exams(db, skip, limit)
