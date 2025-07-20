
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi import HTTPException, status
from app.db.models.result import Result as ResultModel
from app.db.schemas.result import ResultCreate, Result
from uuid6 import uuid6

async def create_result(db: AsyncSession, result: ResultCreate) -> Result:
    existing_result = await db.execute(select(ResultModel).filter(ResultModel.exam_id == result.exam_id, ResultModel.student_global_id == result.student_global_id, ResultModel.centre_number == result.centre_number))
    if existing_result.scalars().first():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Result already exists for this exam, student, and centre"
        )
    result_data = result.model_dump()
    result_data["id"] = str(uuid6())
    db_result = ResultModel(**result_data)
    db.add(db_result)
    await db.commit()
    await db.refresh(db_result)
    return Result.model_validate(db_result)

async def get_result(db: AsyncSession, result_id: str) -> Result:
    result = await db.execute(select(ResultModel).filter(ResultModel.id == result_id))
    result_obj = result.scalars().first()
    if not result_obj:
        raise HTTPException(status_code=404, detail="Result not found")
    return Result.model_validate(result_obj)

async def get_results(db: AsyncSession, skip: int = 0, limit: int = 100) -> list[Result]:
    result = await db.execute(select(ResultModel).offset(skip).limit(limit))
    return [Result.model_validate(result_obj) for result_obj in result.scalars().all()]
