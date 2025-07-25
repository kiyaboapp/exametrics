
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.api.deps import get_current_user, get_db
from app.db.models.user import User
from app.db.models.result import Result
from app.db.schemas.result import ResultCreate, Result
from app.services.result_service import create_result, get_result, get_results,prepare_results_df,execute_results_insert
from typing import List
import pandas as pd
import time

router = APIRouter(prefix="/results", tags=["results"])

@router.post("/", response_model=Result)
async def create_result_endpoint(result: ResultCreate, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await create_result(db, result)

@router.get("/{result_id}", response_model=Result)
async def get_result_endpoint(result_id: str, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await get_result(db, result_id)

@router.get("/", response_model=List[Result])
async def get_results_endpoint(skip: int = 0, limit: int = 100, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await get_results(db, skip, limit)


