import pytest
import pytest_asyncio
from unittest.mock import AsyncMock
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.models.exam_division import ExamDivision
from app.schemas.exam_division import ExamDivisionCreate, ExamDivision
from app.services.exam_division_service import create_exam_division, get_exam_division, get_exam_divisions
from uuid6 import uuid6

@pytest_asyncio.fixture
async def division_data():
    return ExamDivisionCreate(
        exam_id=str(uuid6()),
        division="I",
        lowest_points=7,
        highest_points=17
    )

@pytest.mark.asyncio
async def test_create_exam_division_success(db_session: AsyncSession, division_data: ExamDivisionCreate):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    db_division = ExamDivision(**division_data.dict())
    db_session.add.return_value = None
    db_session.commit.return_value = None
    db_session.refresh.return_value = None
    
    result = await create_exam_division(db_session, division_data)
    
    assert isinstance(result, ExamDivision)
    assert result.division == division_data.division
    db_session.add.assert_called()
    db_session.commit.assert_called()
    db_session.refresh.assert_called()

@pytest.mark.asyncio
async def test_create_exam_division_conflict(db_session: AsyncSession, division_data: ExamDivisionCreate):
    db_session.execute.return_value.scalars.return_value.first.return_value = ExamDivision(**division_data.dict())
    
    with pytest.raises(HTTPException) as exc:
        await create_exam_division(db_session, division_data)
    
    assert exc.value.status_code == 409
    assert "already exists" in exc.value.detail

@pytest.mark.asyncio
async def test_create_exam_division_invalid_range(db_session: AsyncSession, division_data: ExamDivisionCreate):
    division_data.lowest_points = 20
    division_data.highest_points = 10
    
    with pytest.raises(HTTPException) as exc:
        await create_exam_division(db_session, division_data)
    
    assert exc.value.status_code == 400
    assert "lowest_points" in exc.value.detail

@pytest.mark.asyncio
async def test_get_exam_division_success(db_session: AsyncSession):
    exam_id = str(uuid6())
    db_division = ExamDivision(exam_id=exam_id, division="I", lowest_points=7, highest_points=17)
    db_session.execute.return_value.scalars.return_value.first.return_value = db_division
    
    result = await get_exam_division(db_session, exam_id, "I")
    
    assert isinstance(result, ExamDivision)
    assert result.exam_id == exam_id
    db_session.execute.assert_called()

@pytest.mark.asyncio
async def test_get_exam_division_not_found(db_session: AsyncSession):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    
    with pytest.raises(HTTPException) as exc:
        await get_exam_division(db_session, str(uuid6()), "I")
    
    assert exc.value.status_code == 404
    assert "not found" in exc.value.detail

@pytest.mark.asyncio
async def test_get_exam_divisions(db_session: AsyncSession):
    db_division = ExamDivision(exam_id=str(uuid6()), division="I", lowest_points=7, highest_points=17)
    db_session.execute.return_value.scalars.return_value.all.return_value = [db_division]
    
    result = await get_exam_divisions(db_session, str(uuid6()))
    
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], ExamDivision)
    db_session.execute.assert_called()