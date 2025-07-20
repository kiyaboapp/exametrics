
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.api.deps import get_current_user, get_db
from app.db.models.user import User
from app.db.schemas.student_subject import StudentSubjectCreate, StudentSubject
from app.services.student_subject_service import create_student_subject, get_student_subject, get_student_subjects
from typing import List

router = APIRouter(prefix="/student-subjects", tags=["student-subjects"])

@router.post("/", response_model=StudentSubject)
async def create_student_subject_endpoint(student_subject: StudentSubjectCreate, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await create_student_subject(db, student_subject)

@router.get("/{student_subject_id}", response_model=StudentSubject)
async def get_student_subject_endpoint(student_subject_id: str, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await get_student_subject(db, student_subject_id)

@router.get("/", response_model=List[StudentSubject])
async def get_student_subjects_endpoint(skip: int = 0, limit: int = 100, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await get_student_subjects(db, skip, limit)
