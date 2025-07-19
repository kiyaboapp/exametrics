import pytest
import pytest_asyncio
from unittest.mock import AsyncMock
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.models.exam import Exam, ExamLevel
from app.db.models.exam_division import ExamDivision
from app.db.models.exam_grade import ExamGrade
from app.schemas.exam import ExamCreate, Exam
from app.services.exam_service import create_exam, get_exam, get_exams
from uuid6 import uuid6

@pytest_asyncio.fixture
async def exam_data():
    return ExamCreate(
        exam_name="Test Exam",
        start_date="2025-01-01",
        end_date="2025-01-10",
        avg_style="AUTO",
        exam_level=ExamLevel.CSEE,
        board_id=str(uuid6())
    )

@pytest.mark.asyncio
async def test_create_exam_success(db_session: AsyncSession, exam_data: ExamCreate):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    db_exam = Exam(**exam_data.dict(), exam_id=str(uuid6()))
    db_session.add.return_value = None
    db_session.commit.return_value = None
    db_session.refresh.return_value = None
    
    result = await create_exam(db_session, exam_data)
    
    assert isinstance(result, Exam)
    assert result.exam_name == exam_data.exam_name
    assert result.exam_id is not None
    db_session.add.assert_called()
    db_session.commit.assert_called()
    db_session.refresh.assert_called()

@pytest.mark.asyncio
async def test_create_exam_conflict(db_session: AsyncSession, exam_data: ExamCreate):
    db_session.execute.return_value.scalars.return_value.first.return_value = Exam(**exam_data.dict())
    
    with pytest.raises(HTTPException) as exc:
        await create_exam(db_session, exam_data)
    
    assert exc.value.status_code == 409
    assert "already exists" in exc.value.detail

@pytest.mark.asyncio
async def test_get_exam_success(db_session: AsyncSession):
    exam_id = str(uuid6())
    db_exam = Exam(exam_id=exam_id, exam_name="Test Exam", start_date="2025-01-01", end_date="2025-01-10", avg_style="AUTO", exam_level=ExamLevel.CSEE, board_id=str(uuid6()))
    db_session.execute.return_value.scalars.return_value.first.return_value = db_exam
    
    result = await get_exam(db_session, exam_id)
    
    assert isinstance(result, Exam)
    assert result.exam_id == exam_id
    db_session.execute.assert_called()

@pytest.mark.asyncio
async def test_get_exam_not_found(db_session: AsyncSession):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    
    with pytest.raises(HTTPException) as exc:
        await get_exam(db_session, str(uuid6()))
    
    assert exc.value.status_code == 404
    assert "not found" in exc.value.detail

@pytest.mark.asyncio
async def test_get_exams(db_session: AsyncSession):
    db_exam = Exam(exam_id=str(uuid6()), exam_name="Test Exam", start_date="2025-01-01", end_date="2025-01-10", avg_style="AUTO", exam_level=ExamLevel.CSEE, board_id=str(uuid6()))
    db_session.execute.return_value.scalars.return_value.all.return_value = [db_exam]
    
    result = await get_exams(db_session)
    
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], Exam)
    db_session.execute.assert_called()