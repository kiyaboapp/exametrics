import pytest
import pytest_asyncio
from unittest.mock import AsyncMock
from httpx import AsyncClient
from app.db.models.subject import Subject
from app.schemas.subject import SubjectCreate

@pytest_asyncio.fixture
async def subject_data():
    return {
        "subject_code": "MATH",
        "subject_name": "Mathematics",
        "subject_short": "Math"
    }

@pytest.mark.asyncio
async def test_create_subject_success(client: AsyncClient, db_session: AsyncSession, subject_data):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    db_session.add.return_value = None
    db_session.commit.return_value = None
    db_session.refresh.return_value = None
    
    response = await client.post("/api/v1/subjects/", json=subject_data)
    
    assert response.status_code == 200
    assert response.json()["subject_code"] == subject_data["subject_code"]

@pytest.mark.asyncio
async def test_get_subject_success(client: AsyncClient, db_session: AsyncSession):
    subject_code = "MATH"
    db_subject = Subject(subject_code=subject_code, subject_name="Mathematics", subject_short="Math")
    db_session.execute.return_value.scalars.return_value.first.return_value = db_subject
    
    response = await client.get(f"/api/v1/subjects/{subject_code}")
    
    assert response.status_code == 200
    assert response.json()["subject_code"] == subject_code

@pytest.mark.asyncio
async def test_get_subject_not_found(client: AsyncClient, db_session: AsyncSession):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    
    response = await client.get("/api/v1/subjects/MATH")
    
    assert response.status_code == 404
    assert "not found" in response.json()["detail"]

@pytest.mark.asyncio
async def test_get_subjects(client: AsyncClient, db_session: AsyncSession):
    db_subject = Subject(subject_code="MATH", subject_name="Mathematics", subject_short="Math")
    db_session.execute.return_value.scalars.return_value.all.return_value = [db_subject]
    
    response = await client.get("/api/v1/subjects/")
    
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["subject_name"] == "Mathematics"