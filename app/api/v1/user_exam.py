
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.api.deps import get_current_user, get_db
from app.db.models.user import User
from app.db.schemas.user_exam import UserExamCreate, UserExam
from app.services.user_exam_service import create_user_exam, get_user_exam, get_user_exams
from typing import List

router = APIRouter(prefix="/user-exams", tags=["user-exams"])

@router.post("/", response_model=UserExam)
async def create_user_exam_endpoint(user_exam: UserExamCreate, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await create_user_exam(db, user_exam)

@router.get("/{user_id}/{exam_id}", response_model=UserExam)
async def get_user_exam_endpoint(user_id: str, exam_id: str, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await get_user_exam(db, user_id, exam_id)

@router.get("/", response_model=List[UserExam])
async def get_user_exams_endpoint(skip: int = 0, limit: int = 100, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await get_user_exams(db, skip, limit)
