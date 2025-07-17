from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi import Depends, HTTPException, status
from app.db.models.result import Result
from app.schemas.result import ResultCreate, ResultInDB
from app.db.database import get_db

async def create_result(db: AsyncSession, result: ResultCreate) -> ResultInDB:
    db_result = Result(**result.dict())
    db.add(db_result)
    await db.commit()
    await db.refresh(db_result)
    return ResultInDB.from_orm(db_result)

async def get_result(db: AsyncSession, exam_id: str, student_id: str) -> ResultInDB:
    result = await db.execute(select(Result).filter(Result.exam_id == exam_id, Result.student_id == student_id))
    result = result.scalars().first()
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    return ResultInDB.from_orm(result)