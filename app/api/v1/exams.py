# app/api/v1/exams.py

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.database import get_db
from app.schemas.exam import ExamCreate, ExamInDB
from app.services.exam_service import create_exam, get_exam

router = APIRouter()

@router.post("/", response_model=ExamInDB)
async def create_new_exam(exam: ExamCreate, db: AsyncSession = Depends(get_db)):
    return await create_exam(db, exam)

@router.get("/{exam_id}", response_model=ExamInDB)
async def get_exam_by_id(exam_id: str, db: AsyncSession = Depends(get_db)):
    return await get_exam(db, exam_id)