
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi import HTTPException, status
from app.db.models.examination_board import ExamBoard as ExamBoardModel
from app.db.schemas.examination_board import ExamBoardCreate, ExamBoard
from uuid6 import uuid6

async def create_exam_board(db: AsyncSession, exam_board: ExamBoardCreate) -> ExamBoard:
    existing_board = await db.execute(select(ExamBoardModel).filter(ExamBoardModel.name == exam_board.name))
    if existing_board.scalars().first():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Exam board already exists with this name"
        )
    board_data = exam_board.model_dump()
    board_data["board_id"] = str(uuid6())
    db_board = ExamBoardModel(**board_data)
    db.add(db_board)
    await db.commit()
    await db.refresh(db_board)
    return ExamBoard.model_validate(db_board)

async def get_exam_board(db: AsyncSession, board_id: str) -> ExamBoard:
    result = await db.execute(select(ExamBoardModel).filter(ExamBoardModel.board_id == board_id))
    board = result.scalars().first()
    if not board:
        raise HTTPException(status_code=404, detail="Exam board not found")
    return ExamBoard.model_validate(board)

async def get_exam_boards(db: AsyncSession, skip: int = 0, limit: int = 100) -> list[ExamBoard]:
    result = await db.execute(select(ExamBoardModel).offset(skip).limit(limit))
    return [ExamBoard.model_validate(board) for board in result.scalars().all()]
