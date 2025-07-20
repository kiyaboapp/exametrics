
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi import HTTPException, status
from app.db.models.exam_division import ExamDivision as ExamDivisionModel
from app.db.schemas.exam_division import ExamDivisionCreate, ExamDivision
from uuid6 import uuid6

async def create_exam_division(db: AsyncSession, exam_division: ExamDivisionCreate) -> ExamDivision:
    existing_division = await db.execute(select(ExamDivisionModel).filter(ExamDivisionModel.exam_id == exam_division.exam_id, ExamDivisionModel.division == exam_division.division))
    if existing_division.scalars().first():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Exam division already exists for this exam and division"
        )
    division_data = exam_division.model_dump()
    division_data["id"] = division_data.get("id", 0)  # Auto-incremented by DB
    db_division = ExamDivisionModel(**division_data)
    db.add(db_division)
    await db.commit()
    await db.refresh(db_division)
    return ExamDivision.model_validate(db_division)

async def get_exam_division(db: AsyncSession, exam_division_id: int) -> ExamDivision:
    result = await db.execute(select(ExamDivisionModel).filter(ExamDivisionModel.id == exam_division_id))
    division = result.scalars().first()
    if not division:
        raise HTTPException(status_code=404, detail="Exam division not found")
    return ExamDivision.model_validate(division)

async def get_exam_divisions(db: AsyncSession, skip: int = 0, limit: int = 100) -> list[ExamDivision]:
    result = await db.execute(select(ExamDivisionModel).offset(skip).limit(limit))
    return [ExamDivision.model_validate(division) for division in result.scalars().all()]
