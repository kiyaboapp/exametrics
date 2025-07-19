import pytest
import pytest_asyncio
from unittest.mock import AsyncMock
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.models.school import School, SchoolType
from app.schemas.school import SchoolCreate, School
from app.services.school_service import create_school, get_school, get_schools

@pytest_asyncio.fixture
async def school_data():
    return SchoolCreate(
        school_name="Test School",
        centre_number="C001",
        school_type=SchoolType.GOVERNMENT
    )

@pytest.mark.asyncio
async def test_create_school_success(db_session: AsyncSession, school_data: SchoolCreate):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    db_school = School(**school_data.dict())
    db_session.add.return_value = None
    db_session.commit.return_value = None
    db_session.refresh.return_value = None
    
    result = await create_school(db_session, school_data)
    
    assert isinstance(result, School)
    assert result.centre_number == school_data.centre_number
    db_session.add.assert_called()
    db_session.commit.assert_called()
    db_session.refresh.assert_called()

@pytest.mark.asyncio
async def test_create_school_conflict(db_session: AsyncSession, school_data: SchoolCreate):
    db_session.execute.return_value.scalars.return_value.first.return_value = School(**school_data.dict())
    
    with pytest.raises(HTTPException) as exc:
        await create_school(db_session, school_data)
    
    assert exc.value.status_code == 409
    assert "already exists" in exc.value.detail

@pytest.mark.asyncio
async def test_get_school_success(db_session: AsyncSession):
    centre_number = "C001"
    db_school = School(centre_number=centre_number, school_name="Test School", school_type=SchoolType.GOVERNMENT)
    db_session.execute.return_value.scalars.return_value.first.return_value = db_school
    
    result = await get_school(db_session, centre_number)
    
    assert isinstance(result, School)
    assert result.centre_number == centre_number
    db_session.execute.assert_called()

@pytest.mark.asyncio
async def test_get_school_not_found(db_session: AsyncSession):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    
    with pytest.raises(HTTPException) as exc:
        await get_school(db_session, "C001")
    
    assert exc.value.status_code == 404
    assert "not found" in exc.value.detail

@pytest.mark.asyncio
async def test_get_schools(db_session: AsyncSession):
    db_school = School(centre_number="C001", school_name="Test School", school_type=SchoolType.GOVERNMENT)
    db_session.execute.return_value.scalars.return_value.all.return_value = [db_school]
    
    result = await get_schools(db_session)
    
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], School)
    db_session.execute.assert_called()