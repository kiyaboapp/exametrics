
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.api.deps import get_current_user, get_db
from app.db.models.user import User
from app.db.schemas.council import CouncilCreate, Council
from app.services.council_service import create_council, get_council, get_councils
from typing import List

router = APIRouter(prefix="/councils", tags=["councils"])

@router.post("/", response_model=Council)
async def create_council_endpoint(council: CouncilCreate, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await create_council(db, council)

@router.get("/{council_id}", response_model=Council)
async def get_council_endpoint(council_id: int, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await get_council(db, council_id)

@router.get("/", response_model=List[Council])
async def get_councils_endpoint(skip: int = 0, limit: int = 100, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await get_councils(db, skip, limit)
