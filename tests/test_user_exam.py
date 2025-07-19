import pytest
import pytest_asyncio
from unittest.mock import AsyncMock
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.models.user_exam import UserExam
from app.schemas.user_exam import UserExamCreate, UserExam
from app.services.user_exam_service import create_user_exam, get_user_exam, get_user_exams
from uuid6 import uuid6

@pytest_asyncio.fixture
async def user_exam_data():
    return UserExamCreate(
        user_id=str(uuid6()),
        exam_id=str(uuid6()),
        role="VIEWER",
        permissions={"can_view": True}
    )

@pytest.mark.asyncio
async def test_create_user_exam_success(db_session: AsyncSession, user_exam_data: UserExamCreate):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    db_user_exam = UserExam(**user_exam_data.dict())
    db_session.add.return_value = None
    db_session.commit.return_value = None
    db_session.refresh.return_value = None
    
    result = await create_user_exam(db_session, user_exam_data)
    
    assert isinstance(result, UserExam)
    assert result.user_id == user_exam_data.user_id
    db_session.add.assert_called()
    db_session.commit.assert_called()
    db_session.refresh.assert_called()

@pytest.mark.asyncio
async def test_create_user_exam_conflict(db_session: AsyncSession, user_exam_data: UserExamCreate):
    db_session.execute.return_value.scalars.return_value.first.return_value = UserExam(**user_exam_data.dict())
    
    with pytest.raises(HTTPException) as exc:
        await create_user_exam(db_session, user_exam_data)
    
    assert exc.value.status_code == 409
    assert "already exists" in exc.value.detail

@pytest.mark.asyncio
async def test_get_user_exam_success(db_session: AsyncSession):
    user_id = str(uuid6())
    exam_id = str(uuid6())
    db_user_exam = UserExam(user_id=user_id, exam_id=exam_id, role="VIEWER", permissions={"can_view": True})
    db_session.execute.return_value.scalars.return_value.first.return_value = db_user_exam
    
    result = await get_user_exam(db_session, user_id, exam_id)
    
    assert isinstance(result, UserExam)
    assert result.user_id == user_id
    db_session.execute.assert_called()

@pytest.mark.asyncio
async def test_get_user_exam_not_found(db_session: AsyncSession):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    
    with pytest.raises(HTTPException) as exc:
        await get_user_exam(db_session, str(uuid6()), str(uuid6()))
    
    assert exc.value.status_code == 404
    assert "not found" in exc.value.detail

@pytest.mark.asyncio
async def test_get_user_exams(db_session: AsyncSession):
    db_user_exam = UserExam(user_id=str(uuid6()), exam_id=str(uuid6()), role="VIEWER", permissions={"can_view": True})
    db_session.execute.return_value.scalars.return_value.all.return_value = [db_user_exam]
    
    result = await get_user_exams(db_session, str(uuid6()))
    
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], UserExam)
    db_session.execute.assert_called()