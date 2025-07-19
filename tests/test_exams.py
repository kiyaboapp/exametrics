import pytest
import pytest_asyncio
from unittest.mock import AsyncMock
from httpx import AsyncClient
from app.db.models.exam import Exam, ExamLevel
from app.schemas.exam import ExamCreate
from uuid6 import uuid6

@pytest_asyncio.fixture
async def exam_data():
    return {
        "exam_name": "Test Exam",
        "start_date": "2025-01-01",
        "end_date": "2025-01-10",
        "avg_style": "AUTO",
        "exam_level": "CSEE",
        "board_id": str(uuid6())
    }

@pytest.mark.asyncio
async def test_create_exam_success(client: AsyncClient, db_session: AsyncSession, exam_data):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    db_session.add.return_value = None
    db_session.commit.return_value = None
    db_session.refresh.return_value = None
    
    response = await client.post("/api/v1/exams/", json=exam_data)
    
    assert response.status_code == 200
    assert response.json()["exam_name"] == exam_data["exam_name"]

@pytest.mark.asyncio
async def test_get_exam_success(client: AsyncClient, db_session: AsyncSession):
    exam_id = str(uuid6())
    db_exam = Exam(exam_id=exam_id, exam_name="Test Exam", start_date="2025-01-01", end_date="2025-01-10", avg_style="AUTO", exam_level=ExamLevel.CSEE, board_id=str(uuid6()))
    db_session.execute.return_value.scalars.return_value.first.return_value = db_exam
    
    response = await client.get(f"/api/v1/exams/{exam_id}")
    
    assert response.status_code == 200
    assert response.json()["exam_id"] == exam_id

@pytest.mark.asyncio
async def test_get_exam_not_found(client: AsyncClient, db_session: AsyncSession):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    
    response = await client.get(f"/api/v1/exams/{str(uuid6())}")
    
    assert response.status_code == 404
    assert "not found" in response.json()["detail"]

@pytest.mark.asyncio
async def test_get_exams(client: AsyncClient, db_session: AsyncSession):
    db_exam = Exam(exam_id=str(uuid6()), exam_name="Test Exam", start_date="2025-01-01", end_date="2025-01-10", avg_style="AUTO", exam_level=ExamLevel.CSEE, board_id=str(uuid6()))
    db_session.execute.return_value.scalars.return_value.all.return_value = [db_exam]
    
    response = await client.get("/api/v1/exams/")
    
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["exam_name"] == "Test Exam"