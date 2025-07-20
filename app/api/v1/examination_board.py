
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.api.deps import get_current_user, get_db
from app.db.models.user import User
from app.db.schemas.examination_board import ExamBoardCreate, ExamBoard
from app.services.examination_board_service import create_exam_board, get_exam_board, get_exam_boards
from typing import List

router = APIRouter(prefix="/exam-boards", tags=["exam-boards"])

@router.post("/", response_model=ExamBoard)
async def create_exam_board_endpoint(exam_board: ExamBoardCreate, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await create_exam_board(db, exam_board)

@router.get("/{board_id}", response_model=ExamBoard)
async def get_exam_board_endpoint(board_id: str, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await get_exam_board(db, board_id)

@router.get("/", response_model=List[ExamBoard])
async def get_exam_boards_endpoint(skip: int = 0, limit: int = 100, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await get_exam_boards(db, skip, limit)
