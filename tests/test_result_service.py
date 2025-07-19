import pytest
import pytest_asyncio
from unittest.mock import AsyncMock
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.models.result import Result
from app.schemas.result import ResultCreate, Result
from app.services.result_service import create_result, get_result, get_results
from uuid6 import uuid6

@pytest_asyncio.fixture
async def result_data():
    return ResultCreate(
        exam_id=str(uuid6()),
        student_global_id=str(uuid6()),
        centre_number="C001",
        avg_marks=85.0
    )

@pytest.mark.asyncio
async def test_create_result_success(db_session: AsyncSession, result_data: ResultCreate):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    db_result = Result(**result_data.dict(), id=str(uuid6()))
    db_session.add.return_value = None
    db_session.commit.return_value = None
    db_session.refresh.return_value = None
    
    result = await create_result(db_session, result_data)
    
    assert isinstance(result, Result)
    assert result.exam_id == result_data.exam_id
    assert result.id is not None
    db_session.add.assert_called()
    db_session.commit.assert_called()
    db_session.refresh.assert_called()

@pytest.mark.asyncio
async def test_create_result_conflict(db_session: AsyncSession, result_data: ResultCreate):
    db_session.execute.return_value.scalars.return_value.first.return_value = Result(**result_data.dict())
    
    with pytest.raises(HTTPException) as exc:
        await create_result(db_session, result_data)
    
    assert exc.value.status_code == 409
    assert "already exists" in exc.value.detail

@pytest.mark.asyncio
async def test_get_result_success(db_session: AsyncSession):
    result_id = str(uuid6())
    db_result = Result(id=result_id, exam_id=str(uuid6()), student_global_id=str(uuid6()), centre_number="C001")
    db_session.execute.return_value.scalars.return_value.first.return_value = db_result
    
    result = await get_result(db_session, result_id)
    
    assert isinstance(result, Result)
    assert result.id == result_id
    db_session.execute.assert_called()

@pytest.mark.asyncio
async def test_get_result_not_found(db_session: AsyncSession):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    
    with pytest.raises(HTTPException) as exc:
        await get_result(db_session, str(uuid6()))
    
    assert exc.value.status_code == 404
    assert "not found" in exc.value.detail

@pytest.mark.asyncio
async def test_get_results(db_session: AsyncSession):
    db_result = Result(id=str(uuid6()), exam_id=str(uuid6()), student_global_id=str(uuid6()), centre_number="C001")
    db_session.execute.return_value.scalars.return_value.all.return_value = [db_result]
    
    result = await get_results(db_session)
    
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], Result)
    db_session.execute.assert_called()