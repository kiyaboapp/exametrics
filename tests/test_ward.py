
import pytest
import httpx
from fastapi import status
from app.db.schemas.ward import WardCreate
from app.db.models.ward import Ward as WardModel

@pytest.mark.asyncio
async def test_create_ward(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    ward_data = {"ward_id": 1, "ward_name": "Igogo", "council_id": 1}
    response = await client.post("/api/v1/wards/", json=ward_data, headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["ward_name"] == "Igogo"

@pytest.mark.asyncio
async def test_create_ward_conflict(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    ward_data = {"ward_id": 1, "ward_name": "Igogo", "council_id": 1}
    await client.post("/api/v1/wards/", json=ward_data, headers=headers)
    response = await client.post("/api/v1/wards/", json=ward_data, headers=headers)
    assert response.status_code == status.HTTP_409_CONFLICT

@pytest.mark.asyncio
async def test_get_ward(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    ward = WardModel(ward_id=1, ward_name="Igogo", council_id=1)
    async_session.add(ward)
    await async_session.commit()
    response = await client.get("/api/v1/wards/1", headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["ward_name"] == "Igogo"

@pytest.mark.asyncio
async def test_get_ward_not_found(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    response = await client.get("/api/v1/wards/999", headers=headers)
    assert response.status_code == status.HTTP_404_NOT_FOUND

@pytest.mark.asyncio
async def test_get_wards(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    wards = [
        WardModel(ward_id=1, ward_name="Igogo", council_id=1),
        WardModel(ward_id=2, ward_name="Pamba", council_id=1)
    ]
    async_session.add_all(wards)
    await async_session.commit()
    response = await client.get("/api/v1/wards/", headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data) == 2
    assert data[0]["ward_name"] == "Igogo"

@pytest.mark.asyncio
async def test_create_ward_unauthorized(client):
    ward_data = {"ward_id": 1, "ward_name": "Igogo", "council_id": 1}
    response = await client.post("/api/v1/wards/", json=ward_data)
    assert response.status_code == status.HTTP_401_UNAUTHORIZED
