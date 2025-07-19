import pytest
import pytest_asyncio
from unittest.mock import AsyncMock
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.models.subject import Subject
from app.schemas.subject import SubjectCreate, Subject
from app.services.subject_service import create_subject, get_subject, get_subjects

@pytest_asyncio.fixture
async def subject_data():
    return SubjectCreate(
        subject_code="MATH",
        subject_name="Mathematics",
        subject_short="Math"
    )

@pytest.mark.asyncio
async def test_create_subject_success(db_session: AsyncSession, subject_data: SubjectCreate):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    db_subject = Subject(**subject_data.dict())
    db_session.add.return_value = None
    db_session.commit.return_value = None
    db_session.refresh.return_value = None
    
    result = await create_subject(db_session, subject_data)
    
    assert isinstance(result, Subject)
    assert result.subject_code == subject_data.subject_code
    db_session.add.assert_called()
    db_session.commit.assert_called()
    db_session.refresh.assert_called()

@pytest.mark.asyncio
async def test_create_subject_conflict(db_session: AsyncSession, subject_data: SubjectCreate):
    db_session.execute.return_value.scalars.return_value.first.return_value = Subject(**subject_data.dict())
    
    with pytest.raises(HTTPException) as exc:
        await create_subject(db_session, subject_data)
    
    assert exc.value.status_code == 409
    assert "already exists" in exc.value.detail

@pytest.mark.asyncio
async def test_get_subject_success(db_session: AsyncSession):
    subject_code = "MATH"
    db_subject = Subject(subject_code=subject_code, subject_name="Mathematics", subject_short="Math")
    db_session.execute.return_value.scalars.return_value.first.return_value = db_subject
    
    result = await get_subject(db_session, subject_code)
    
    assert isinstance(result, Subject)
    assert result.subject_code == subject_code
    db_session.execute.assert_called()

@pytest.mark.asyncio
async def test_get_subject_not_found(db_session: AsyncSession):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    
    with pytest.raises(HTTPException) as exc:
        await get_subject(db_session, "MATH")
    
    assert exc.value.status_code == 404
    assert "not found" in exc.value.detail

@pytest.mark.asyncio
async def test_get_subjects(db_session: AsyncSession):
    db_subject = Subject(subject_code="MATH", subject_name="Mathematics", subject_short="Math")
    db_session.execute.return_value.scalars.return_value.all.return_value = [db_subject]
    
    result = await get_subjects(db_session)
    
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], Subject)
    db_session.execute.assert_called()