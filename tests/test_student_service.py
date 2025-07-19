import pytest
import pytest_asyncio
from unittest.mock import AsyncMock
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.models.student import Student
from app.schemas.student import StudentCreate, Student
from app.services.student_service import create_student, get_student, get_students
from uuid6 import uuid6

@pytest_asyncio.fixture
async def student_data():
    return StudentCreate(
        student_id="S123",
        centre_number="C001",
        first_name="John",
        surname="Doe",
        sex="M",
        exam_id=str(uuid6())
    )

@pytest.mark.asyncio
async def test_create_student_success(db_session: AsyncSession, student_data: StudentCreate):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    db_student = Student(**student_data.dict(), student_global_id=str(uuid6()))
    db_session.add.return_value = None
    db_session.commit.return_value = None
    db_session.refresh.return_value = None
    
    result = await create_student(db_session, student_data)
    
    assert isinstance(result, Student)
    assert result.student_id == student_data.student_id
    assert result.student_global_id is not None
    db_session.add.assert_called()
    db_session.commit.assert_called()
    db_session.refresh.assert_called()

@pytest.mark.asyncio
async def test_create_student_conflict(db_session: AsyncSession, student_data: StudentCreate):
    db_session.execute.return_value.scalars.return_value.first.return_value = Student(**student_data.dict())
    
    with pytest.raises(HTTPException) as exc:
        await create_student(db_session, student_data)
    
    assert exc.value.status_code == 409
    assert "already exists" in exc.value.detail

@pytest.mark.asyncio
async def test_get_student_success(db_session: AsyncSession):
    student_global_id = str(uuid6())
    db_student = Student(student_global_id=student_global_id, student_id="S123", centre_number="C001", first_name="John", surname="Doe", sex="M", exam_id=str(uuid6()))
    db_session.execute.return_value.scalars.return_value.first.return_value = db_student
    
    result = await get_student(db_session, student_global_id)
    
    assert isinstance(result, Student)
    assert result.student_global_id == student_global_id
    db_session.execute.assert_called()

@pytest.mark.asyncio
async def test_get_student_not_found(db_session: AsyncSession):
    db_session.execute.return_value.scalars.return_value.first.return_value = None
    
    with pytest.raises(HTTPException) as exc:
        await get_student(db_session, str(uuid6()))
    
    assert exc.value.status_code == 404
    assert "not found" in exc.value.detail

@pytest.mark.asyncio
async def test_get_students(db_session: AsyncSession):
    db_student = Student(student_global_id=str(uuid6()), student_id="S123", centre_number="C001", first_name="John", surname="Doe", sex="M", exam_id=str(uuid6()))
    db_session.execute.return_value.scalars.return_value.all.return_value = [db_student]
    
    result = await get_students(db_session)
    
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], Student)
    db_session.execute.assert_called()