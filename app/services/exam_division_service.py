# app/services/exam_division_service.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi import HTTPException, status
from app.db.models.exam_division import ExamDivision
from app.schemas.exam_division import ExamDivisionCreate, ExamDivision

async def create_exam_division(db: AsyncSession, division: ExamDivisionCreate) -> ExamDivision:
    # Validate points range
    if division.lowest_points > division.highest_points:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="lowest_points must be less than or equal to highest_points"
        )
    
    existing_division = await db.execute(select(ExamDivision).filter(
        ExamDivision.exam_id == division.exam_id,
        ExamDivision.division == division.division
    ))
    if existing_division.scalars().first():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Division already exists for this exam"
        )
    
    db_division = ExamDivision(**division.dict())
    db.add(db_division)
    await db.commit()
    await db.refresh(db_division)
    return ExamDivision.from_orm(db_division)

async def get_exam_division(db: AsyncSession, exam_id: str, division: str) -> ExamDivision:
    result = await db.execute(select(ExamDivision).filter(
        ExamDivision.exam_id == exam_id,
        ExamDivision.division == division
    ))
    division = result.scalars().first()
    if not division:
        raise HTTPException(status_code=404, detail="Exam division not found")
    return ExamDivision.from_orm(division)

async def get_exam_divisions(db: AsyncSession, exam_id: str) -> list[ExamDivision]:
    result = await db.execute(select(ExamDivision).filter(ExamDivision.exam_id == exam_id))
    return [ExamDivision.from_orm(division) for division in result.scalars().all()]