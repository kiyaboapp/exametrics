import pytest
import pytest_asyncio
from unittest.mock import AsyncMock
from httpx import AsyncClient
from app.db.models.school import School, SchoolType
from app.schemas.school import SchoolCreate

@pytest_asyncio.fixture
async def school_data():
    return {
        "school_name": "Test School",
        "centre_number": "C001",
        "school_type": "GOVERNMENT"
    }

@pytest.mark.asyncio
async def test_create_school_success(client: AsyncClient, db_session: AsyncSession, school_data):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    db_session.add.return_value = None
    db_session.commit.return_value = None
    db_session.refresh.return_value = None
    
    response = await client.post("/api/v1/schools/", json=school_data)
    
    assert response.status_code == 200
    assert response.json()["centre_number"] == school_data["centre_number"]

@pytest.mark.asyncio
async def test_get_school_success(client: AsyncClient, db_session: AsyncSession):
    centre_number = "C001"
    db_school = School(centre_number=centre_number, school_name="Test School", school_type=SchoolType.GOVERNMENT)
    db_session.execute.return_value.scalars.return_value.first.return_value = db_school
    
    response = await client.get(f"/api/v1/schools/{centre_number}")
    
    assert response.status_code == 200
    assert response.json()["centre_number"] == centre_number

@pytest.mark.asyncio
async def test_get_school_not_found(client: AsyncClient, db_session: AsyncSession):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    
    response = await client.get("/api/v1/schools/C001")
    
    assert response.status_code == 404
    assert "not found" in response.json()["detail"]

@pytest.mark.asyncio
async def test_get_schools(client: AsyncClient, db_session: AsyncSession):
    db_school = School(centre_number="C001", school_name="Test School", school_type=SchoolType.GOVERNMENT)
    db_session.execute.return_value.scalars.return_value.all.return_value = [db_school]
    
    response = await client.get("/api/v1/schools/")
    
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["school_name"] == "Test School"