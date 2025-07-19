# app/api/v1/exam_divisions.py

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.database import get_db
from app.schemas.exam_division import ExamDivisionCreate, ExamDivision
from app.services.exam_division_service import create_exam_division, get_exam_division, get_exam_divisions

router = APIRouter()

@router.post("/", response_model=ExamDivision)
async def create_new_exam_division(division: ExamDivisionCreate, db: AsyncSession = Depends(get_db)):
    return await create_exam_division(db, division)

@router.get("/{exam_id}/{division}", response_model=ExamDivision)
async def get_exam_division_by_id(exam_id: str, division: str, db: AsyncSession = Depends(get_db)):
    return await get_exam_division(db, exam_id, division)

@router.get("/{exam_id}", response_model=list[ExamDivision])
async def get_exam_divisions_by_exam(exam_id: str, db: AsyncSession = Depends(get_db)):
    return await get_exam_divisions(db, exam_id)