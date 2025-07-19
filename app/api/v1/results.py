# app/api/v1/results.py

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.database import get_db
from app.schemas.result import ResultCreate, Result
from app.services.result_service import create_result, get_result, get_results

router = APIRouter()

@router.post("/", response_model=Result)
async def create_new_result(result: ResultCreate, db: AsyncSession = Depends(get_db)):
    return await create_result(db, result)

@router.get("/{result_id}", response_model=Result)
async def get_result_by_id(result_id: str, db: AsyncSession = Depends(get_db)):
    return await get_result(db, result_id)

@router.get("/", response_model=list[Result])
async def get_all_results(db: AsyncSession = Depends(get_db), skip: int = 0, limit: int = 100):
    return await get_results(db, skip, limit)