import pytest
import pytest_asyncio
from unittest.mock import AsyncMock
from httpx import AsyncClient
from app.db.models.exam_grade import ExamGrade
from app.schemas.exam_grade import ExamGradeCreate
from uuid6 import uuid6

@pytest_asyncio.fixture
async def grade_data():
    return {
        "exam_id": str(uuid6()),
        "grade": "A",
        "lower_value": 75.0,
        "highest_value": 100.0,
        "grade_points": 1.0,
        "division_points": 1
    }

@pytest.mark.asyncio
async def test_create_exam_grade_success(client: AsyncClient, db_session: AsyncSession, grade_data):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    db_session.add.return_value = None
    db_session.commit.return_value = None
    db_session.refresh.return_value = None
    
    response = await client.post("/api/v1/exam_grades/", json=grade_data)
    
    assert response.status_code == 200
    assert response.json()["grade"] == grade_data["grade"]

@pytest.mark.asyncio
async def test_get_exam_grade_success(client: AsyncClient, db_session: AsyncSession):
    exam_id = str(uuid6())
    grade = "A"
    db_grade = ExamGrade(exam_id=exam_id, grade=grade, lower_value=75.0, highest_value=100.0, grade_points=1.0, division_points=1)
    db_session.execute.return_value.scalars.return_value.first.return_value = db_grade
    
    response = await client.get(f"/api/v1/exam_grades/{exam_id}/{grade}")
    
    assert response.status_code == 200
    assert response.json()["exam_id"] == exam_id

@pytest.mark.asyncio
async def test_get_exam_grade_not_found(client: AsyncClient, db_session: AsyncSession):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    
    response = await client.get(f"/api/v1/exam_grades/{str(uuid6())}/A")
    
    assert response.status_code == 404
    assert "not found" in response.json()["detail"]

@pytest.mark.asyncio
async def test_get_exam_grades_by_exam(client: AsyncClient, db_session: AsyncSession):
    db_grade = ExamGrade(exam_id=str(uuid6()), grade="A", lower_value=75.0, highest_value=100.0, grade_points=1.0, division_points=1)
    db_session.execute.return_value.scalars.return_value.all.return_value = [db_grade]
    
    response = await client.get(f"/api/v1/exam_grades/{str(uuid6())}")
    
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["grade"] == "A"