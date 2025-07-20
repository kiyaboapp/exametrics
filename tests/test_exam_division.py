
import pytest
import httpx
from fastapi import status
from app.db.schemas.exam_division import ExamDivisionCreate
from app.db.models.exam_division import ExamDivision as ExamDivisionModel
from uuid6 import uuid6

@pytest.mark.asyncio
async def test_create_exam_division(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    division_data = {"id": 1, "exam_id": str(uuid6()), "division": "I", "lowest_points": 7, "highest_points": 17}
    response = await client.post("/api/v1/exam-divisions/", json=division_data, headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["division"] == "I"

@pytest.mark.asyncio
async def test_create_exam_division_conflict(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    exam_id = str(uuid6())
    division_data = {"id": 1, "exam_id": exam_id, "division": "I", "lowest_points": 7, "highest_points": 17}
    await client.post("/api/v1/exam-divisions/", json=division_data, headers=headers)
    response = await client.post("/api/v1/exam-divisions/", json={"id": 2, "exam_id": exam_id, "division": "I", "lowest_points": 7, "highest_points": 17}, headers=headers)
    assert response.status_code == status.HTTP_409_CONFLICT

@pytest.mark.asyncio
async def test_get_exam_division(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    division = ExamDivisionModel(id=1, exam_id=str(uuid6()), division="I", lowest_points=7, highest_points=17)
    async_session.add(division)
    await async_session.commit()
    response = await client.get("/api/v1/exam-divisions/1", headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["division"] == "I"

@pytest.mark.asyncio
async def test_get_exam_division_not_found(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    response = await client.get("/api/v1/exam-divisions/999", headers=headers)
    assert response.status_code == status.HTTP_404_NOT_FOUND

@pytest.mark.asyncio
async def test_get_exam_divisions(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    exam_id = str(uuid6())
    divisions = [
        ExamDivisionModel(id=1, exam_id=exam_id, division="I", lowest_points=7, highest_points=17),
        ExamDivisionModel(id=2, exam_id=exam_id, division="II", lowest_points=18, highest_points=22)
    ]
    async_session.add_all(divisions)
    await async_session.commit()
    response = await client.get("/api/v1/exam-divisions/", headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data) == 2
    assert data[0]["division"] == "I"

@pytest.mark.asyncio
async def test_create_exam_division_unauthorized(client):
    division_data = {"id": 1, "exam_id": str(uuid6()), "division": "I", "lowest_points": 7, "highest_points": 17}
    response = await client.post("/api/v1/exam-divisions/", json=division_data)
    assert response.status_code == status.HTTP_401_UNAUTHORIZED
