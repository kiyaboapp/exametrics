import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, patch
from fastapi import status
from httpx import AsyncClient
from app.db.models.user import User
from app.db.models.school import School
from app.schemas.user import UserCreate, User

@pytest_asyncio.fixture
async def user_data():
    return {
        "username": "testuser",
        "email": "test@example.com",
        "first_name": "Test",
        "surname": "User",
        "role": "USER",
        "centre_number": "C001",
        "password": "password123"
    }

@pytest.mark.asyncio
async def test_register_user_success(client: AsyncClient, db_session: AsyncSession, user_data):
    db_session.execute.side_effect = [
        AsyncMock(scalars=AsyncMock(first=AsyncMock(return_value=None))),  # User check
        AsyncMock(scalars=AsyncMock(first=AsyncMock(return_value=School(centre_number="C001"))))  # School check
    ]
    db_session.add.return_value = None
    db_session.commit.return_value = None
    db_session.refresh.return_value = None
    
    with patch("app.core.security.get_password_hash", return_value="hashed_password"):
        response = await client.post("/api/v1/auth/register", json=user_data)
    
    assert response.status_code == 200
    assert response.json()["username"] == user_data["username"]

@pytest.mark.asyncio
async def test_register_user_conflict(client: AsyncClient, db_session: AsyncSession, user_data):
    db_session.execute.return_value.scalars.return_value.first.return_value = User(**user_data)
    
    response = await client.post("/api/v1/auth/register", json=user_data)
    
    assert response.status_code == 400
    assert "already registered" in response.json()["detail"]

@pytest.mark.asyncio
async def test_login_success(client: AsyncClient, db_session: AsyncSession):
    db_user = User(username="testuser", hashed_password="hashed_password")
    db_session.execute.return_value.scalars.return_value.first.return_value = db_user
    
    with patch("app.core.security.verify_password", return_value=True):
        with patch("app.core.security.create_access_token", return_value="test_token"):
            response = await client.post("/api/v1/auth/login", data={"username": "testuser", "password": "password123"})
    
    assert response.status_code == 200
    assert response.json()["access_token"] == "test_token"
    assert response.json()["token_type"] == "bearer"

@pytest.mark.asyncio
async def test_login_invalid_credentials(client: AsyncClient, db_session: AsyncSession):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    
    response = await client.post("/api/v1/auth/login", data={"username": "testuser", "password": "password123"})
    
    assert response.status_code == 401
    assert "Incorrect" in response.json()["detail"]