
import pytest
import httpx
from fastapi import status
from app.db.schemas.examination_board import ExamBoardCreate
from app.db.models.examination_board import ExamBoard as ExamBoardModel
from uuid6 import uuid6

@pytest.mark.asyncio
async def test_create_exam_board(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    board_data = {"board_id": str(uuid6()), "name": "NECTA"}
    response = await client.post("/api/v1/exam-boards/", json=board_data, headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["name"] == "NECTA"

@pytest.mark.asyncio
async def test_create_exam_board_conflict(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    board_data = {"board_id": str(uuid6()), "name": "NECTA"}
    await client.post("/api/v1/exam-boards/", json=board_data, headers=headers)
    response = await client.post("/api/v1/exam-boards/", json={"board_id": str(uuid6()), "name": "NECTA"}, headers=headers)
    assert response.status_code == status.HTTP_409_CONFLICT

@pytest.mark.asyncio
async def test_get_exam_board(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    board_id = str(uuid6())
    board = ExamBoardModel(board_id=board_id, name="NECTA")
    async_session.add(board)
    await async_session.commit()
    response = await client.get(f"/api/v1/exam-boards/{board_id}", headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["name"] == "NECTA"

@pytest.mark.asyncio
async def test_get_exam_board_not_found(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    response = await client.get(f"/api/v1/exam-boards/{str(uuid6())}", headers=headers)
    assert response.status_code == status.HTTP_404_NOT_FOUND

@pytest.mark.asyncio
async def test_get_exam_boards(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    boards = [
        ExamBoardModel(board_id=str(uuid6()), name="NECTA"),
        ExamBoardModel(board_id=str(uuid6()), name="Cambridge")
    ]
    async_session.add_all(boards)
    await async_session.commit()
    response = await client.get("/api/v1/exam-boards/", headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data) == 2
    assert data[0]["name"] == "NECTA"

@pytest.mark.asyncio
async def test_create_exam_board_unauthorized(client):
    board_data = {"board_id": str(uuid6()), "name": "NECTA"}
    response = await client.post("/api/v1/exam-boards/", json=board_data)
    assert response.status_code == status.HTTP_401_UNAUTHORIZED
