
import pytest
import httpx
from fastapi import status
from app.db.schemas.exam import ExamCreate
from app.db.models.exam import Exam as ExamModel, ExamLevel
from uuid6 import uuid6

@pytest.mark.asyncio
async def test_create_exam(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    exam_data = {"exam_id": str(uuid6()), "exam_name": "CSEE 2025", "board_id": str(uuid6()), "exam_level": "CSEE"}
    response = await client.post("/api/v1/exams/", json=exam_data, headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["exam_name"] == "CSEE 2025"
    assert data["exam_level"] == "CSEE"

@pytest.mark.asyncio
async def test_create_exam_conflict(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    board_id = str(uuid6())
    exam_data = {"exam_id": str(uuid6()), "exam_name": "CSEE 2025", "board_id": board_id, "exam_level": "CSEE"}
    await client.post("/api/v1/exams/", json=exam_data, headers=headers)
    response = await client.post("/api/v1/exams/", json={"exam_id": str(uuid6()), "exam_name": "CSEE 2025", "board_id": board_id, "exam_level": "CSEE"}, headers=headers)
    assert response.status_code == status.HTTP_409_CONFLICT

@pytest.mark.asyncio
async def test_get_exam(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    exam_id = str(uuid6())
    exam = ExamModel(exam_id=exam_id, exam_name="CSEE 2025", board_id=str(uuid6()), exam_level=ExamLevel.CSEE)
    async_session.add(exam)
    await async_session.commit()
    response = await client.get(f"/api/v1/exams/{exam_id}", headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["exam_name"] == "CSEE 2025"

@pytest.mark.asyncio
async def test_get_exam_not_found(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    response = await client.get(f"/api/v1/exams/{str(uuid6())}", headers=headers)
    assert response.status_code == status.HTTP_404_NOT_FOUND

@pytest.mark.asyncio
async def test_get_exams(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    exams = [
        ExamModel(exam_id=str(uuid6()), exam_name="CSEE 2025", board_id=str(uuid6()), exam_level=ExamLevel.CSEE),
        ExamModel(exam_id=str(uuid6()), exam_name="FTNA 2025", board_id=str(uuid6()), exam_level=ExamLevel.FTNA)
    ]
    async_session.add_all(exams)
    await async_session.commit()
    response = await client.get("/api/v1/exams/", headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data) == 2
    assert data[0]["exam_name"] == "CSEE 2025"

@pytest.mark.asyncio
async def test_create_exam_unauthorized(client):
    exam_data = {"exam_id": str(uuid6()), "exam_name": "CSEE 2025", "board_id": str(uuid6()), "exam_level": "CSEE"}
    response = await client.post("/api/v1/exams/", json=exam_data)
    assert response.status_code == status.HTTP_401_UNAUTHORIZED
