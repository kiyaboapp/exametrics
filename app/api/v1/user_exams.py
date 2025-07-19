# app/api/v1/user_exams.py

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.database import get_db
from app.schemas.user_exam import UserExamCreate, UserExam
from app.services.user_exam_service import create_user_exam, get_user_exam, get_user_exams

router = APIRouter()

@router.post("/", response_model=UserExam)
async def create_new_user_exam(user_exam: UserExamCreate, db: AsyncSession = Depends(get_db)):
    return await create_user_exam(db, user_exam)

@router.get("/{user_id}/{exam_id}", response_model=UserExam)
async def get_user_exam_by_id(user_id: str, exam_id: str, db: AsyncSession = Depends(get_db)):
    return await get_user_exam(db, user_id, exam_id)

@router.get("/{user_id}", response_model=list[UserExam])
async def get_user_exams_by_user(user_id: str, db: AsyncSession = Depends(get_db)):
    return await get_user_exams(db, user_id)