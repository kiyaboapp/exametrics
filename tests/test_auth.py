
import pytest
import httpx
from fastapi import status

@pytest.mark.asyncio
async def test_login_success(client, test_user, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    print(headers)
    response = await client.get(f"/api/v1/users/{test_user.id}", headers=headers)
    print(response)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["username"] == test_user.username

@pytest.mark.asyncio
async def test_login_invalid_credentials(client):
    response = await client.post(
        "/api/v1/auth/login",
        data={"username": "wronguser", "password": "wrongpassword"}
    )
    assert response.status_code == status.HTTP_401_UNAUTHORIZED
