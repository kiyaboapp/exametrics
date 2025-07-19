import pytest
import pytest_asyncio
from unittest.mock import AsyncMock
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.models.exam_subject import ExamSubject
from app.schemas.exam_subject import ExamSubjectCreate, ExamSubject
from app.services.exam_subject_service import create_exam_subject, get_exam_subject, get_exam_subjects
from uuid6 import uuid6

@pytest_asyncio.fixture
async def exam_subject_data():
    return ExamSubjectCreate(
        exam_id=str(uuid6()),
        subject_code="MATH",
        subject_name="Mathematics",
        subject_short="Math",
        has_practical=False,
        exclude_from_gpa=False
    )

@pytest.mark.asyncio
async def test_create_exam_subject_success(db_session: AsyncSession, exam_subject_data: ExamSubjectCreate):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    db_subject = ExamSubject(**exam_subject_data.dict())
    db_session.add.return_value = None
    db_session.commit.return_value = None
    db_session.refresh.return_value = None
    
    result = await create_exam_subject(db_session, exam_subject_data)
    
    assert isinstance(result, ExamSubject)
    assert result.subject_code == exam_subject_data.subject_code
    db_session.add.assert_called()
    db_session.commit.assert_called()
    db_session.refresh.assert_called()

@pytest.mark.asyncio
async def test_create_exam_subject_conflict(db_session: AsyncSession, exam_subject_data: ExamSubjectCreate):
    db_session.execute.return_value.scalars.return_value.first.return_value = ExamSubject(**exam_subject_data.dict())
    
    with pytest.raises(HTTPException) as exc:
        await create_exam_subject(db_session, exam_subject_data)
    
    assert exc.value.status_code == 409
    assert "already exists" in exc.value.detail

@pytest.mark.asyncio
async def test_get_exam_subject_success(db_session: AsyncSession):
    exam_id = str(uuid6())
    subject_code = "MATH"
    db_subject = ExamSubject(exam_id=exam_id, subject_code=subject_code, subject_name="Mathematics", subject_short="Math")
    db_session.execute.return_value.scalars.return_value.first.return_value = db_subject
    
    result = await get_exam_subject(db_session, exam_id, subject_code)
    
    assert isinstance(result, ExamSubject)
    assert result.exam_id == exam_id
    db_session.execute.assert_called()

@pytest.mark.asyncio
async def test_get_exam_subject_not_found(db_session: AsyncSession):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    
    with pytest.raises(HTTPException) as exc:
        await get_exam_subject(db_session, str(uuid6()), "MATH")
    
    assert exc.value.status_code == 404
    assert "not found" in exc.value.detail

@pytest.mark.asyncio
async def test_get_exam_subjects(db_session: AsyncSession):
    db_subject = ExamSubject(exam_id=str(uuid6()), subject_code="MATH", subject_name="Mathematics", subject_short="Math")
    db_session.execute.return_value.scalars.return_value.all.return_value = [db_subject]
    
    result = await get_exam_subjects(db_session, str(uuid6()))
    
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], ExamSubject)
    db_session.execute.assert_called()