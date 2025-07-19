# app/services/result_service.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi import HTTPException, status
from app.db.models.result import Result
from app.schemas.result import ResultCreate, Result
from uuid6 import uuid6

async def create_result(db: AsyncSession, result: ResultCreate) -> Result:
    # Check for existing result with composite key
    existing_result = await db.execute(select(Result).filter(
        Result.exam_id == result.exam_id,
        Result.student_global_id == result.student_global_id,
        Result.centre_number == result.centre_number
    ))
    if existing_result.scalars().first():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Result already exists for this exam, student, and school"
        )
    
    db_result = Result(**result.dict(), id=str(uuid6()))
    db.add(db_result)
    await db.commit()
    await db.refresh(db_result)
    return Result.from_orm(db_result)

async def get_result(db: AsyncSession, result_id: str) -> Result:
    result = await db.execute(select(Result).filter(Result.id == result_id))
    result = result.scalars().first()
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    return Result.from_orm(result)

async def get_results(db: AsyncSession, skip: int = 0, limit: int = 100) -> list[Result]:
    result = await db.execute(select(Result).offset(skip).limit(limit))
    return [Result.from_orm(result) for result in result.scalars().all()]