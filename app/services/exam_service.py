from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi import Depends, HTTPException, status
from app.db.models.exam import Exam
from app.schemas.exam import ExamCreate, ExamInDB
from app.db.database import get_db

async def create_exam(db: AsyncSession, exam: ExamCreate) -> ExamInDB:
    db_exam = Exam(**exam.dict())
    db.add(db_exam)
    await db.commit()
    await db.refresh(db_exam)
    return ExamInDB.from_orm(db_exam)

async def get_exam(db: AsyncSession, exam_id: str) -> ExamInDB:
    result = await db.execute(select(Exam).filter(Exam.exam_id == exam_id))
    exam = result.scalars().first()
    if not exam:
        raise HTTPException(status_code=404, detail="Exam not found")
    return ExamInDB.from_orm(exam)