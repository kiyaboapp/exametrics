# app/services/exam_division_service.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi import HTTPException
from app.db.models.exam_division import ExamDivision
from app.schemas.exam_division import ExamDivisionCreate, ExamDivisionInDB

async def create_exam_division(db: AsyncSession, division: ExamDivisionCreate) -> ExamDivisionInDB:
    db_division = ExamDivision(**division.dict())
    db.add(db_division)
    await db.commit()
    await db.refresh(db_division)
    return ExamDivisionInDB.from_orm(db_division)

async def get_exam_division(db: AsyncSession, exam_id: str, division: str) -> ExamDivisionInDB:
    result = await db.execute(
        select(ExamDivision).filter(
            ExamDivision.exam_id == exam_id,
            ExamDivision.division == division
        )
    )
    division = result.scalars().first()
    if not division:
        raise HTTPException(status_code=404, detail="Exam division not found")
    return ExamDivisionInDB.from_orm(division)