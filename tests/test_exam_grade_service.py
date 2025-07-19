import pytest
import pytest_asyncio
from unittest.mock import AsyncMock
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.models.exam_grade import ExamGrade
from app.schemas.exam_grade import ExamGradeCreate, ExamGrade
from app.services.exam_grade_service import create_exam_grade, get_exam_grade, get_exam_grades
from uuid6 import uuid6

@pytest_asyncio.fixture
async def grade_data():
    return ExamGradeCreate(
        exam_id=str(uuid6()),
        grade="A",
        lower_value=75.0,
        highest_value=100.0,
        grade_points=1.0,
        division_points=1
    )

@pytest.mark.asyncio
async def test_create_exam_grade_success(db_session: AsyncSession, grade_data: ExamGradeCreate):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    db_grade = ExamGrade(**grade_data.dict())
    db_session.add.return_value = None
    db_session.commit.return_value = None
    db_session.refresh.return_value = None
    
    result = await create_exam_grade(db_session, grade_data)
    
    assert isinstance(result, ExamGrade)
    assert result.grade == grade_data.grade
    db_session.add.assert_called()
    db_session.commit.assert_called()
    db_session.refresh.assert_called()

@pytest.mark.asyncio
async def test_create_exam_grade_conflict(db_session: AsyncSession, grade_data: ExamGradeCreate):
    db_session.execute.return_value.scalars.return_value.first.return_value = ExamGrade(**grade_data.dict())
    
    with pytest.raises(HTTPException) as exc:
        await create_exam_grade(db_session, grade_data)
    
    assert exc.value.status_code == 409
    assert "already exists" in exc.value.detail

@pytest.mark.asyncio
async def test_create_exam_grade_invalid_range(db_session: AsyncSession, grade_data: ExamGradeCreate):
    grade_data.lower_value = 100.0
    grade_data.highest_value = 75.0
    
    with pytest.raises(HTTPException) as exc:
        await create_exam_grade(db_session, grade_data)
    
    assert exc.value.status_code == 400
    assert "lower_value" in exc.value.detail

@pytest.mark.asyncio
async def test_get_exam_grade_success(db_session: AsyncSession):
    exam_id = str(uuid6())
    db_grade = ExamGrade(exam_id=exam_id, grade="A", lower_value=75.0, highest_value=100.0, grade_points=1.0, division_points=1)
    db_session.execute.return_value.scalars.return_value.first.return_value = db_grade
    
    result = await get_exam_grade(db_session, exam_id, "A")
    
    assert isinstance(result, ExamGrade)
    assert result.exam_id == exam_id
    db_session.execute.assert_called()

@pytest.mark.asyncio
async def test_get_exam_grade_not_found(db_session: AsyncSession):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    
    with pytest.raises(HTTPException) as exc:
        await get_exam_grade(db_session, str(uuid6()), "A")
    
    assert exc.value.status_code == 404
    assert "not found" in exc.value.detail

@pytest.mark.asyncio
async def test_get_exam_grades(db_session: AsyncSession):
    db_grade = ExamGrade(exam_id=str(uuid6()), grade="A", lower_value=75.0, highest_value=100.0, grade_points=1.0, division_points=1)
    db_session.execute.return_value.scalars.return_value.all.return_value = [db_grade]
    
    result = await get_exam_grades(db_session, str(uuid6()))
    
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], ExamGrade)
    db_session.execute.assert_called()