import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession
from unittest.mock import AsyncMock, patch
from app.main import app
from app.db.database import get_db

@pytest_asyncio.fixture
async def db_session():
    session = AsyncMock(spec=AsyncSession)
    return session

@pytest_asyncio.fixture
async def client():
    async def override_get_db():
        session = AsyncMock(spec=AsyncSession)
        yield session
    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.clear()