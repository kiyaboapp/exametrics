# app/api/v1/subjects.py

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.database import get_db
from app.schemas.subject import SubjectCreate, Subject
from app.services.subject_service import create_subject, get_subject, get_subjects

router = APIRouter()

@router.post("/", response_model=Subject)
async def create_new_subject(subject: SubjectCreate, db: AsyncSession = Depends(get_db)):
    return await create_subject(db, subject)

@router.get("/{subject_code}", response_model=Subject)
async def get_subject_by_code(subject_code: str, db: AsyncSession = Depends(get_db)):
    return await get_subject(db, subject_code)

@router.get("/", response_model=list[Subject])
async def get_all_subjects(db: AsyncSession = Depends(get_db), skip: int = 0, limit: int = 100):
    return await get_subjects(db, skip, limit)