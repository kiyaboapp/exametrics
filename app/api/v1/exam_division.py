# app/api/v1/exam_divisions.py

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.api.deps import get_current_user, get_db
from app.db.models.user import User
from app.db.schemas.exam_division import ExamDivisionCreate, ExamDivision
from app.services.exam_division_service import create_exam_division, get_exam_division, get_exam_divisions
from typing import List

router = APIRouter(prefix="/exam-divisions", tags=["exam-divisions"])

@router.post("/", response_model=ExamDivision)
async def create_exam_division_endpoint(exam_division: ExamDivisionCreate, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await create_exam_division(db, exam_division)

@router.get("/{exam_division_id}", response_model=ExamDivision)
async def get_exam_division_endpoint(exam_division_id: int, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await get_exam_division(db, exam_division_id)

@router.get("/", response_model=List[ExamDivision])
async def get_exam_divisions_endpoint(skip: int = 0, limit: int = 100, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await get_exam_divisions(db, skip, limit)
