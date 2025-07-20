
import pytest
import httpx
from fastapi import status
from app.db.schemas.user_exam import UserExamCreate
from app.db.models.user_exam import UserExam as UserExamModel
from uuid6 import uuid6

@pytest.mark.asyncio
async def test_create_user_exam(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    user_exam_data = {"user_id": str(uuid6()), "exam_id": str(uuid6())}
    response = await client.post("/api/v1/user-exams/", json=user_exam_data, headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["user_id"] == user_exam_data["user_id"]
    assert data["exam_id"] == user_exam_data["exam_id"]

@pytest.mark.asyncio
async def test_create_user_exam_conflict(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    user_id = str(uuid6())
    exam_id = str(uuid6())
    user_exam_data = {"user_id": user_id, "exam_id": exam_id}
    await client.post("/api/v1/user-exams/", json=user_exam_data, headers=headers)
    response = await client.post("/api/v1/user-exams/", json=user_exam_data, headers=headers)
    assert response.status_code == status.HTTP_409_CONFLICT

@pytest.mark.asyncio
async def test_get_user_exam(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    user_id = str(uuid6())
    exam_id = str(uuid6())
    user_exam = UserExamModel(user_id=user_id, exam_id=exam_id)
    async_session.add(user_exam)
    await async_session.commit()
    response = await client.get(f"/api/v1/user-exams/{user_id}/{exam_id}", headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["user_id"] == user_id
    assert data["exam_id"] == exam_id

@pytest.mark.asyncio
async def test_get_user_exam_not_found(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    response = await client.get(f"/api/v1/user-exams/{str(uuid6())}/{str(uuid6())}", headers=headers)
    assert response.status_code == status.HTTP_404_NOT_FOUND

@pytest.mark.asyncio
async def test_get_user_exams(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    user_exams = [
        UserExamModel(user_id=str(uuid6()), exam_id=str(uuid6())),
        UserExamModel(user_id=str(uuid6()), exam_id=str(uuid6()))
    ]
    async_session.add_all(user_exams)
    await async_session.commit()
    response = await client.get("/api/v1/user-exams/", headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data) == 2

@pytest.mark.asyncio
async def test_create_user_exam_unauthorized(client):
    user_exam_data = {"user_id": str(uuid6()), "exam_id": str(uuid6())}
    response = await client.post("/api/v1/user-exams/", json=user_exam_data)
    assert response.status_code == status.HTTP_401_UNAUTHORIZED
