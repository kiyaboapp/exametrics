import pytest
import pytest_asyncio
from unittest.mock import AsyncMock
from httpx import AsyncClient
from app.db.models.exam_division import ExamDivision
from app.schemas.exam_division import ExamDivisionCreate
from uuid6 import uuid6

@pytest_asyncio.fixture
async def division_data():
    return {
        "exam_id": str(uuid6()),
        "division": "I",
        "lowest_points": 7,
        "highest_points": 17
    }

@pytest.mark.asyncio
async def test_create_exam_division_success(client: AsyncClient, db_session: AsyncSession, division_data):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    db_session.add.return_value = None
    db_session.commit.return_value = None
    db_session.refresh.return_value = None
    
    response = await client.post("/api/v1/exam_divisions/", json=division_data)
    
    assert response.status_code == 200
    assert response.json()["division"] == division_data["division"]

@pytest.mark.asyncio
async def test_get_exam_division_success(client: AsyncClient, db_session: AsyncSession):
    exam_id = str(uuid6())
    division = "I"
    db_division = ExamDivision(exam_id=exam_id, division=division, lowest_points=7, highest_points=17)
    db_session.execute.return_value.scalars.return_value.first.return_value = db_division
    
    response = await client.get(f"/api/v1/exam_divisions/{exam_id}/{division}")
    
    assert response.status_code == 200
    assert response.json()["exam_id"] == exam_id

@pytest.mark.asyncio
async def test_get_exam_division_not_found(client: AsyncClient, db_session: AsyncSession):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    
    response = await client.get(f"/api/v1/exam_divisions/{str(uuid6())}/I")
    
    assert response.status_code == 404
    assert "not found" in response.json()["detail"]

@pytest.mark.asyncio
async def test_get_exam_divisions_by_exam(client: AsyncClient, db_session: AsyncSession):
    db_division = ExamDivision(exam_id=str(uuid6()), division="I", lowest_points=7, highest_points=17)
    db_session.execute.return_value.scalars.return_value.all.return_value = [db_division]
    
    response = await client.get(f"/api/v1/exam_divisions/{str(uuid6())}")
    
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["division"] == "I"