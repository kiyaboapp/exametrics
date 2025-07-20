
import pytest
import asyncio
import sys
import os
from pathlib import Path

# Add project root to sys.path
project_root = str(Path(__file__).parent.parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from app.main import app
from app.db.base import Base
from app.db.models.user import User, Role
from app.core.config import settings
from app.core.security import get_password_hash
from jose import jwt
from uuid6 import uuid6

# Set environment variables for testing
os.environ["PROJECT_NAME"] = "EXAMETRICS API Test"
os.environ["API_V1_STR"] = "/api/v1"
os.environ["SECRET_KEY"] = "test-secret-key"
os.environ["ALGORITHM"] = "HS256"
os.environ["ACCESS_TOKEN_EXPIRE_MINUTES"] = "30"
os.environ["ALLOWED_ORIGINS"] = "http://localhost:3000,http://127.0.0.1:3000"
os.environ["DB_HOST"] = "localhost"
os.environ["DB_PORT"] = "3306"
os.environ["DB_USER"] = "testuser"
os.environ["DB_PASSWORD"] = "testpassword"
os.environ["DB_NAME"] = "testdb"

@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture(scope="session")
async def async_engine():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()

@pytest.fixture
async def async_session(async_engine):
    async_session = async_sessionmaker(bind=async_engine, class_=AsyncSession, expire_on_commit=False)
    async with async_session() as session:
        yield session

@pytest.fixture
async def client():
    return TestClient(app)

@pytest.fixture
async def test_user(async_session: AsyncSession):
    user = User(
        id=str(uuid6()),
        username="testuser",
        email="test@example.com",
        first_name="Test",
        surname="User",
        role=Role.ADMIN,
        hashed_password=get_password_hash("testpassword"),
        is_active=True,
        is_verified=True,
        centre_number=None
    )
    async_session.add(user)
    await async_session.commit()
    await async_session.refresh(user)
    return user

@pytest.fixture
def token(test_user):
    payload = {"sub": test_user.username}
    return jwt.encode(payload, settings.SECRET_KEY, algorithm=settings.ALGORITHM)

@pytest.fixture
async def login_token(client, test_user):
    response = await client.post(
        f"{settings.API_V1_STR}/auth/login",
        data={"username": test_user.username, "password": "testpassword"}
    )
    return response.json()["access_token"]
