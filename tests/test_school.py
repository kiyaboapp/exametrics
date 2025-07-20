
import pytest
import httpx
from fastapi import status
from app.db.schemas.school import SchoolCreate
from app.db.models.school import School as SchoolModel

@pytest.mark.asyncio
async def test_create_school(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    school_data = {"centre_number": "S1869", "school_name": "IGOGO SS", "ward_id": 1}
    response = await client.post("/api/v1/schools/", json=school_data, headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["school_name"] == "IGOGO SS"

@pytest.mark.asyncio
async def test_create_school_conflict(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    school_data = {"centre_number": "S1869", "school_name": "IGOGO SS", "ward_id": 1}
    await client.post("/api/v1/schools/", json=school_data, headers=headers)
    response = await client.post("/api/v1/schools/", json=school_data, headers=headers)
    assert response.status_code == status.HTTP_409_CONFLICT

@pytest.mark.asyncio
async def test_get_school(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    school = SchoolModel(centre_number="S1869", school_name="IGOGO SS", ward_id=1)
    async_session.add(school)
    await async_session.commit()
    response = await client.get("/api/v1/schools/S1869", headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["school_name"] == "IGOGO SS"

@pytest.mark.asyncio
async def test_get_school_not_found(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    response = await client.get("/api/v1/schools/S9999", headers=headers)
    assert response.status_code == status.HTTP_404_NOT_FOUND

@pytest.mark.asyncio
async def test_get_schools(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    schools = [
        SchoolModel(centre_number="S1869", school_name="IGOGO SS", ward_id=1),
        SchoolModel(centre_number="S1870", school_name="PAMBA SS", ward_id=1)
    ]
    async_session.add_all(schools)
    await async_session.commit()
    response = await client.get("/api/v1/schools/", headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data) == 2
    assert data[0]["school_name"] == "IGOGO SS"

@pytest.mark.asyncio
async def test_create_school_unauthorized(client):
    school_data = {"centre_number": "S1869", "school_name": "IGOGO SS", "ward_id": 1}
    response = await client.post("/api/v1/schools/", json=school_data)
    assert response.status_code == status.HTTP_401_UNAUTHORIZED
