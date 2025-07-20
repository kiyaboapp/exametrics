
import pytest
import httpx
from fastapi import status
from app.db.schemas.user import UserCreate
from app.db.models.user import User as UserModel, Role
from uuid6 import uuid6

@pytest.mark.asyncio
async def test_create_user(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    user_data = {
        "id": str(uuid6()),
        "username": "newuser",
        "email": "newuser@example.com",
        "first_name": "New",
        "surname": "User",
        "password": "newpassword",
        "role": "ADMIN",
        "is_active": True,
        "is_verified": True
    }
    response = await client.post("/api/v1/users/", json=user_data, headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["username"] == "newuser"
    assert data["email"] == "newuser@example.com"

@pytest.mark.asyncio
async def test_create_user_conflict(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    user_data = {
        "id": str(uuid6()),
        "username": "newuser",
        "email": "newuser@example.com",
        "first_name": "New",
        "surname": "User",
        "password": "newpassword",
        "role": "ADMIN",
        "is_active": True,
        "is_verified": True
    }
    await client.post("/api/v1/users/", json=user_data, headers=headers)
    response = await client.post("/api/v1/users/", json={**user_data, "id": str(uuid6())}, headers=headers)
    assert response.status_code == status.HTTP_409_CONFLICT

@pytest.mark.asyncio
async def test_get_user(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    user_id = str(uuid6())
    user = UserModel(
        id=user_id,
        username="newuser",
        email="newuser@example.com",
        first_name="New",
        surname="User",
        hashed_password="hashedpassword",
        role=Role.ADMIN,
        is_active=True,
        is_verified=True
    )
    async_session.add(user)
    await async_session.commit()
    response = await client.get(f"/api/v1/users/{user_id}", headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["username"] == "newuser"

@pytest.mark.asyncio
async def test_get_user_not_found(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    response = await client.get(f"/api/v1/users/{str(uuid6())}", headers=headers)
    assert response.status_code == status.HTTP_404_NOT_FOUND

@pytest.mark.asyncio
async def test_get_users(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    users = [
        UserModel(
            id=str(uuid6()),
            username="user1",
            email="user1@example.com",
            first_name="User",
            surname="One",
            hashed_password="hashedpassword",
            role=Role.ADMIN,
            is_active=True,
            is_verified=True
        ),
        UserModel(
            id=str(uuid6()),
            username="user2",
            email="user2@example.com",
            first_name="User",
            surname="Two",
            hashed_password="hashedpassword",
            role=Role.ADMIN,
            is_active=True,
            is_verified=True
        )
    ]
    async_session.add_all(users)
    await async_session.commit()
    response = await client.get("/api/v1/users/", headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data) == 2
    assert data[0]["username"] == "user1"

@pytest.mark.asyncio
async def test_create_user_unauthorized(client):
    user_data = {
        "id": str(uuid6()),
        "username": "newuser",
        "email": "newuser@example.com",
        "first_name": "New",
        "surname": "User",
        "password": "newpassword",
        "role": "ADMIN",
        "is_active": True,
        "is_verified": True
    }
    response = await client.post("/api/v1/users/", json=user_data)
    assert response.status_code == status.HTTP_401_UNAUTHORIZED
