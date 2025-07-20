
import pytest
import httpx
from fastapi import status
from app.db.schemas.result import ResultCreate
from app.db.models.result import Result as ResultModel
from uuid6 import uuid6

@pytest.mark.asyncio
async def test_create_result(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    result_data = {
        "id": str(uuid6()),
        "exam_id": str(uuid6()),
        "student_global_id": str(uuid6()),
        "centre_number": "S1869",
        "division": "I",
        "division_points": 7
    }
    response = await client.post("/api/v1/results/", json=result_data, headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["division"] == "I"
    assert data["centre_number"] == "S1869"

@pytest.mark.asyncio
async def test_create_result_conflict(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    exam_id = str(uuid6())
    student_global_id = str(uuid6())
    result_data = {
        "id": str(uuid6()),
        "exam_id": exam_id,
        "student_global_id": student_global_id,
        "centre_number": "S1869",
        "division": "I",
        "division_points": 7
    }
    await client.post("/api/v1/results/", json=result_data, headers=headers)
    response = await client.post("/api/v1/results/", json={**result_data, "id": str(uuid6())}, headers=headers)
    assert response.status_code == status.HTTP_409_CONFLICT

@pytest.mark.asyncio
async def test_get_result(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    result_id = str(uuid6())
    result = ResultModel(
        id=result_id,
        exam_id=str(uuid6()),
        student_global_id=str(uuid6()),
        centre_number="S1869",
        division="I",
        division_points=7
    )
    async_session.add(result)
    await async_session.commit()
    response = await client.get(f"/api/v1/results/{result_id}", headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["division"] == "I"

@pytest.mark.asyncio
async def test_get_result_not_found(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    response = await client.get(f"/api/v1/results/{str(uuid6())}", headers=headers)
    assert response.status_code == status.HTTP_404_NOT_FOUND

@pytest.mark.asyncio
async def test_get_results(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    results = [
        ResultModel(
            id=str(uuid6()),
            exam_id=str(uuid6()),
            student_global_id=str(uuid6()),
            centre_number="S1869",
            division="I",
            division_points=7
        ),
        ResultModel(
            id=str(uuid6()),
            exam_id=str(uuid6()),
            student_global_id=str(uuid6()),
            centre_number="S1869",
            division="II",
            division_points=18
        )
    ]
    async_session.add_all(results)
    await async_session.commit()
    response = await client.get("/api/v1/results/", headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data) == 2
    assert data[0]["division"] == "I"

@pytest.mark.asyncio
async def test_create_result_unauthorized(client):
    result_data = {
        "id": str(uuid6()),
        "exam_id": str(uuid6()),
        "student_global_id": str(uuid6()),
        "centre_number": "S1869",
        "division": "I",
        "division_points": 7
    }
    response = await client.post("/api/v1/results/", json=result_data)
    assert response.status_code == status.HTTP_401_UNAUTHORIZED
