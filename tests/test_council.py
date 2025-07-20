
import pytest
import httpx
from fastapi import status
from app.db.schemas.council import CouncilCreate
from app.db.models.council import Council as CouncilModel

@pytest.mark.asyncio
async def test_create_council(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    council_data = {"council_id": 1, "council_name": "Mwanza CC", "region_id": 1}
    response = await client.post("/api/v1/councils/", json=council_data, headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["council_name"] == "Mwanza CC"

@pytest.mark.asyncio
async def test_create_council_conflict(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    council_data = {"council_id": 1, "council_name": "Mwanza CC", "region_id": 1}
    await client.post("/api/v1/councils/", json=council_data, headers=headers)
    response = await client.post("/api/v1/councils/", json=council_data, headers=headers)
    assert response.status_code == status.HTTP_409_CONFLICT

@pytest.mark.asyncio
async def test_get_council(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    council = CouncilModel(council_id=1, council_name="Mwanza CC", region_id=1)
    async_session.add(council)
    await async_session.commit()
    response = await client.get("/api/v1/councils/1", headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["council_name"] == "Mwanza CC"

@pytest.mark.asyncio
async def test_get_council_not_found(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    response = await client.get("/api/v1/councils/999", headers=headers)
    assert response.status_code == status.HTTP_404_NOT_FOUND

@pytest.mark.asyncio
async def test_get_councils(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    councils = [
        CouncilModel(council_id=1, council_name="Mwanza CC", region_id=1),
        CouncilModel(council_id=2, council_name="Ilemela", region_id=1)
    ]
    async_session.add_all(councils)
    await async_session.commit()
    response = await client.get("/api/v1/councils/", headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data) == 2
    assert data[0]["council_name"] == "Mwanza CC"

@pytest.mark.asyncio
async def test_create_council_unauthorized(client):
    council_data = {"council_id": 1, "council_name": "Mwanza CC", "region_id": 1}
    response = await client.post("/api/v1/councils/", json=council_data)
    assert response.status_code == status.HTTP_401_UNAUTHORIZED
