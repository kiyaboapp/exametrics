import pytest
import pytest_asyncio
from unittest.mock import AsyncMock
from httpx import AsyncClient
from app.db.models.student import Student
from app.schemas.student import StudentCreate
from uuid6 import uuid6

@pytest_asyncio.fixture
async def student_data():
    return {
        "student_id": "S123",
        "centre_number": "C001",
        "first_name": "John",
        "surname": "Doe",
        "sex": "M",
        "exam_id": str(uuid6())
    }

@pytest.mark.asyncio
async def test_create_student_success(client: AsyncClient, db_session: AsyncSession, student_data):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    db_session.add.return_value = None
    db_session.commit.return_value = None
    db_session.refresh.return_value = None
    
    response = await client.post("/api/v1/students/", json=student_data)
    
    assert response.status_code == 200
    assert response.json()["student_id"] == student_data["student_id"]

@pytest.mark.asyncio
async def test_get_student_success(client: AsyncClient, db_session: AsyncSession):
    student_global_id = str(uuid6())
    db_student = Student(student_global_id=student_global_id, student_id="S123", centre_number="C001", first_name="John", surname="Doe", sex="M", exam_id=str(uuid6()))
    db_session.execute.return_value.scalars.return_value.first.return_value = db_student
    
    response = await client.get(f"/api/v1/students/{student_global_id}")
    
    assert response.status_code == 200
    assert response.json()["student_global_id"] == student_global_id

@pytest.mark.asyncio
async def test_get_student_not_found(client: AsyncClient, db_session: AsyncSession):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    
    response = await client.get(f"/api/v1/students/{str(uuid6())}")
    
    assert response.status_code == 404
    assert "not found" in response.json()["detail"]

@pytest.mark.asyncio
async def test_get_students(client: AsyncClient, db_session: AsyncSession):
    db_student = Student(student_global_id=str(uuid6()), student_id="S123", centre_number="C001", first_name="John", surname="Doe", sex="M", exam_id=str(uuid6()))
    db_session.execute.return_value.scalars.return_value.all.return_value = [db_student]
    
    response = await client.get("/api/v1/students/")
    
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["student_id"] == "S123"