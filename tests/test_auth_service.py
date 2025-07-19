import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, patch
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime
from app.db.models.user import User
from app.db.models.school import School
from app.schemas.user import UserCreate, User
from app.services.auth_service import create_user, authenticate_user
from uuid6 import uuid6

@pytest_asyncio.fixture
async def user_data():
    return UserCreate(
        username="testuser",
        email="test@example.com",
        first_name="Test",
        surname="User",
        role="USER",
        centre_number="C001",
        password="password123"
    )

@pytest.mark.asyncio
async def test_create_user_success(db_session: AsyncSession, user_data: UserCreate):
    db_session.execute.side_effect = [
        AsyncMock(scalars=AsyncMock(first=AsyncMock(return_value=None))),  # User check
        AsyncMock(scalars=AsyncMock(first=AsyncMock(return_value=School(centre_number="C001"))))  # School check
    ]
    db_session.add.return_value = None
    db_session.commit.return_value = None
    db_session.refresh.return_value = None
    
    with patch("app.core.security.get_password_hash", return_value="hashed_password"):
        result = await create_user(db_session, user_data)
    
    assert isinstance(result, User)
    assert result.username == user_data.username
    assert result.id is not None
    db_session.add.assert_called()
    db_session.commit.assert_called()
    db_session.refresh.assert_called()

@pytest.mark.asyncio
async def test_create_user_username_conflict(db_session: AsyncSession, user_data: UserCreate):
    db_session.execute.return_value.scalars.return_value.first.return_value = User(**user_data.dict())
    
    with pytest.raises(HTTPException) as exc:
        await create_user(db_session, user_data)
    
    assert exc.value.status_code == 400
    assert "already registered" in exc.value.detail

@pytest.mark.asyncio
async def test_create_user_invalid_school(db_session: AsyncSession, user_data: UserCreate):
    db_session.execute.side_effect = [
        AsyncMock(scalars=AsyncMock(first=AsyncMock(return_value=None))),  # User check
        AsyncMock(scalars=AsyncMock(first=AsyncMock(return_value=None)))  # School check
    ]
    
    with pytest.raises(HTTPException) as exc:
        await create_user(db_session, user_data)
    
    assert exc.value.status_code == 400
    assert "School" in exc.value.detail

@pytest.mark.asyncio
async def test_authenticate_user_success(db_session: AsyncSession):
    username = "testuser"
    db_user = User(id=str(uuid6()), username=username, hashed_password="hashed_password")
    db_session.execute.return_value.scalars.return_value.first.return_value = db_user
    
    with patch("app.core.security.verify_password", return_value=True):
        result = await authenticate_user(db_session, username, "password123")
    
    assert isinstance(result, User)
    assert result.username == username
    db_session.execute.assert_called()
    db_session.commit.assert_called()

@pytest.mark.asyncio
async def test_authenticate_user_invalid_credentials(db_session: AsyncSession):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    
    with pytest.raises(HTTPException) as exc:
        await authenticate_user(db_session, "testuser", "password123")
    
    assert exc.value.status_code == 401
    assert "Incorrect" in exc.value.detail