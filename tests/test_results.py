import pytest
import pytest_asyncio
from unittest.mock import AsyncMock
from httpx import AsyncClient
from app.db.models.result import Result
from app.schemas.result import ResultCreate
from uuid6 import uuid6

@pytest_asyncio.fixture
async def result_data():
    return {
        "exam_id": str(uuid6()),
        "student_global_id": str(uuid6()),
        "centre_number": "C001",
        "avg_marks": 85.0
    }

@pytest.mark.asyncio
async def test_create_result_success(client: AsyncClient, db_session: AsyncSession, result_data):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    db_session.add.return_value = None
    db_session.commit.return_value = None
    db_session.refresh.return_value = None
    
    response = await client.post("/api/v1/results/", json=result_data)
    
    assert response.status_code == 200
    assert response.json()["exam_id"] == result_data["exam_id"]

@pytest.mark.asyncio
async def test_get_result_success(client: AsyncClient, db_session: AsyncSession):
    result_id = str(uuid6())
    db_result = Result(id=result_id, exam_id=str(uuid6()), student_global_id=str(uuid6()), centre_number="C001")
    db_session.execute.return_value.scalars.return_value.first.return_value = db_result
    
    response = await client.get(f"/api/v1/results/{result_id}")
    
    assert response.status_code == 200
    assert response.json()["id"] == result_id

@pytest.mark.asyncio
async def test_get_result_not_found(client: AsyncClient, db_session: AsyncSession):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    
    response = await client.get(f"/api/v1/results/{str(uuid6())}")
    
    assert response.status_code == 404
    assert "not found" in response.json()["detail"]

@pytest.mark.asyncio
async def test_get_results(client: AsyncClient, db_session: AsyncSession):
    db_result = Result(id=str(uuid6()), exam_id=str(uuid6()), student_global_id=str(uuid6()), centre_number="C001")
    db_session.execute.return_value.scalars.return_value.all.return_value = [db_result]
    
    response = await client.get("/api/v1/results/")
    
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["centre_number"] == "C001"