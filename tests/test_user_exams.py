import pytest
import pytest_asyncio
from unittest.mock import AsyncMock
from httpx import AsyncClient
from app.db.models.user_exam import UserExam
from app.schemas.user_exam import UserExamCreate
from uuid6 import uuid6

@pytest_asyncio.fixture
async def user_exam_data():
    return {
        "user_id": str(uuid6()),
        "exam_id": str(uuid6()),
        "role": "VIEWER",
        "permissions": {"can_view": True}
    }

@pytest.mark.asyncio
async def test_create_user_exam_success(client: AsyncClient, db_session: AsyncSession, user_exam_data):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    db_session.add.return_value = None
    db_session.commit.return_value = None
    db_session.refresh.return_value = None
    
    response = await client.post("/api/v1/user_exams/", json=user_exam_data)
    
    assert response.status_code == 200
    assert response.json()["user_id"] == user_exam_data["user_id"]

@pytest.mark.asyncio
async def test_get_user_exam_success(client: AsyncClient, db_session: AsyncSession):
    user_id = str(uuid6())
    exam_id = str(uuid6())
    db_user_exam = UserExam(user_id=user_id, exam_id=exam_id, role="VIEWER", permissions={"can_view": True})
    db_session.execute.return_value.scalars.return_value.first.return_value = db_user_exam
    
    response = await client.get(f"/api/v1/user_exams/{user_id}/{exam_id}")
    
    assert response.status_code == 200
    assert response.json()["user_id"] == user_id

@pytest.mark.asyncio
async def test_get_user_exam_not_found(client: AsyncClient, db_session: AsyncSession):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    
    response = await client.get(f"/api/v1/user_exams/{str(uuid6())}/{str(uuid6())}")
    
    assert response.status_code == 404
    assert "not found" in response.json()["detail"]

@pytest.mark.asyncio
async def test_get_user_exams_by_user(client: AsyncClient, db_session: AsyncSession):
    db_user_exam = UserExam(user_id=str(uuid6()), exam_id=str(uuid6()), role="VIEWER", permissions={"can_view": True})
    db_session.execute.return_value.scalars.return_value.all.return_value = [db_user_exam]
    
    response = await client.get(f"/api/v1/user_exams/{str(uuid6())}")
    
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["role"] == "VIEWER"