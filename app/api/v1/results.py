from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.database import get_db
from app.schemas.result import ResultCreate, ResultInDB
from app.services.result_service import create_result, get_result

router = APIRouter()

@router.post("/", response_model=ResultInDB)
async def create_new_result(result: ResultCreate, db: AsyncSession = Depends(get_db)):
    return await create_result(db, result)

@router.get("/{exam_id}/{student_id}", response_model=ResultInDB)
async def get_result_by_id(exam_id: str, student_id: str, db: AsyncSession = Depends(get_db)):
    return await get_result(db, exam_id, student_id)