import pytest
import pytest_asyncio
from unittest.mock import AsyncMock
from httpx import AsyncClient
from app.db.models.student_subject import StudentSubject
from app.schemas.student_subject import StudentSubjectCreate
from uuid6 import uuid6

@pytest_asyncio.fixture
async def student_subject_data():
    return {
        "exam_id": str(uuid6()),
        "student_global_id": str(uuid6()),
        "centre_number": "C001",
        "subject_code": "MATH",
        "theory_marks": 80.0
    }

@pytest.mark.asyncio
async def test_create_student_subject_success(client: AsyncClient, db_session: AsyncSession, student_subject_data):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    db_session.add.return_value = None
    db_session.commit.return_value = None
    db_session.refresh.return_value = None
    
    response = await client.post("/api/v1/student_subjects/", json=student_subject_data)
    
    assert response.status_code == 200
    assert response.json()["subject_code"] == student_subject_data["subject_code"]

@pytest.mark.asyncio
async def test_get_student_subject_success(client: AsyncClient, db_session: AsyncSession):
    exam_id = str(uuid6())
    student_global_id = str(uuid6())
    subject_code = "MATH"
    db_subject = StudentSubject(exam_id=exam_id, student_global_id=student_global_id, subject_code=subject_code, centre_number="C001")
    db_session.execute.return_value.scalars.return_value.first.return_value = db_subject
    
    response = await client.get(f"/api/v1/student_subjects/{exam_id}/{student_global_id}/{subject_code}")
    
    assert response.status_code == 200
    assert response.json()["exam_id"] == exam_id

@pytest.mark.asyncio
async def test_get_student_subject_not_found(client: AsyncClient, db_session: AsyncSession):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    
    response = await client.get(f"/api/v1/student_subjects/{str(uuid6())}/{str(uuid6())}/MATH")
    
    assert response.status_code == 404
    assert "not found" in response.json()["detail"]

@pytest.mark.asyncio
async def test_get_student_subjects_by_exam(client: AsyncClient, db_session: AsyncSession):
    db_subject = StudentSubject(exam_id=str(uuid6()), student_global_id=str(uuid6()), subject_code="MATH", centre_number="C001")
    db_session.execute.return_value.scalars.return_value.all.return_value = [db_subject]
    
    response = await client.get(f"/api/v1/student_subjects/{str(uuid6())}")
    
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["subject_code"] == "MATH"