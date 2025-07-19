import pytest
import pytest_asyncio
from unittest.mock import AsyncMock
from httpx import AsyncClient
from app.db.models.exam_subject import ExamSubject
from app.schemas.exam_subject import ExamSubjectCreate
from uuid6 import uuid6

@pytest_asyncio.fixture
async def exam_subject_data():
    return {
        "exam_id": str(uuid6()),
        "subject_code": "MATH",
        "subject_name": "Mathematics",
        "subject_short": "Math",
        "has_practical": False,
        "exclude_from_gpa": False
    }

@pytest.mark.asyncio
async def test_create_exam_subject_success(client: AsyncClient, db_session: AsyncSession, exam_subject_data):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    db_session.add.return_value = None
    db_session.commit.return_value = None
    db_session.refresh.return_value = None
    
    response = await client.post("/api/v1/exam_subjects/", json=exam_subject_data)
    
    assert response.status_code == 200
    assert response.json()["subject_code"] == exam_subject_data["subject_code"]

@pytest.mark.asyncio
async def test_get_exam_subject_success(client: AsyncClient, db_session: AsyncSession):
    exam_id = str(uuid6())
    subject_code = "MATH"
    db_subject = ExamSubject(exam_id=exam_id, subject_code=subject_code, subject_name="Mathematics", subject_short="Math")
    db_session.execute.return_value.scalars.return_value.first.return_value = db_subject
    
    response = await client.get(f"/api/v1/exam_subjects/{exam_id}/{subject_code}")
    
    assert response.status_code == 200
    assert response.json()["exam_id"] == exam_id

@pytest.mark.asyncio
async def test_get_exam_subject_not_found(client: AsyncClient, db_session: AsyncSession):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    
    response = await client.get(f"/api/v1/exam_subjects/{str(uuid6())}/MATH")
    
    assert response.status_code == 404
    assert "not found" in response.json()["detail"]

@pytest.mark.asyncio
async def test_get_exam_subjects_by_exam(client: AsyncClient, db_session: AsyncSession):
    db_subject = ExamSubject(exam_id=str(uuid6()), subject_code="MATH", subject_name="Mathematics", subject_short="Math")
    db_session.execute.return_value.scalars.return_value.all.return_value = [db_subject]
    
    response = await client.get(f"/api/v1/exam_subjects/{str(uuid6())}")
    
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["subject_code"] == "MATH"