
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.api.deps import get_current_user, get_db
from app.db.models.user import User
from app.db.schemas.subject import SubjectCreate, Subject
from app.services.subject_service import create_subject, get_subject, get_subjects
from typing import List

router = APIRouter(prefix="/subjects", tags=["subjects"])

@router.post("/", response_model=Subject)
async def create_subject_endpoint(subject: SubjectCreate, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await create_subject(db, subject)

@router.get("/{subject_code}", response_model=Subject)
async def get_subject_endpoint(subject_code: str, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await get_subject(db, subject_code)

@router.get("/", response_model=List[Subject])
async def get_subjects_endpoint(skip: int = 0, limit: int = 100, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await get_subjects(db, skip, limit)
