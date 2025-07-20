
import pytest
import httpx
from fastapi import status
from app.db.schemas.region import RegionCreate
from app.db.models.region import Region as RegionModel

@pytest.mark.asyncio
async def test_create_region(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    region_data = {"region_id": 1, "region_name": "Mwanza"}
    response = await client.post("/api/v1/regions/", json=region_data, headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["region_name"] == "Mwanza"

@pytest.mark.asyncio
async def test_create_region_conflict(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    region_data = {"region_id": 1, "region_name": "Mwanza"}
    await client.post("/api/v1/regions/", json=region_data, headers=headers)
    response = await client.post("/api/v1/regions/", json=region_data, headers=headers)
    assert response.status_code == status.HTTP_409_CONFLICT

@pytest.mark.asyncio
async def test_get_region(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    region = RegionModel(region_id=1, region_name="Mwanza")
    async_session.add(region)
    await async_session.commit()
    response = await client.get("/api/v1/regions/1", headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["region_name"] == "Mwanza"

@pytest.mark.asyncio
async def test_get_region_not_found(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    response = await client.get("/api/v1/regions/999", headers=headers)
    assert response.status_code == status.HTTP_404_NOT_FOUND

@pytest.mark.asyncio
async def test_get_regions(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    regions = [
        RegionModel(region_id=1, region_name="Mwanza"),
        RegionModel(region_id=2, region_name="Dar es Salaam")
    ]
    async_session.add_all(regions)
    await async_session.commit()
    response = await client.get("/api/v1/regions/", headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data) == 2
    assert data[0]["region_name"] == "Mwanza"

@pytest.mark.asyncio
async def test_create_region_unauthorized(client):
    region_data = {"region_id": 1, "region_name": "Mwanza"}
    response = await client.post("/api/v1/regions/", json=region_data)
    assert response.status_code == status.HTTP_401_UNAUTHORIZED
