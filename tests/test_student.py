
import pytest
import httpx
from fastapi import status
from app.db.schemas.student import StudentCreate
from app.db.models.student import Student as StudentModel
from app.db.models.exam import ExamLevel
from uuid6 import uuid6

@pytest.mark.asyncio
async def test_create_student(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    student_data = {
        "student_global_id": str(uuid6()),
        "exam_id": str(uuid6()),
        "student_id": "S1869-0001",
        "centre_number": "S1869",
        "first_name": "ANNIE",
        "middle_name": "STEPHANO",
        "surname": "SIWITI",
        "sex": "F"
    }
    response = await client.post("/api/v1/students/", json=student_data, headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["student_id"] == "S1869-0001"
    assert data["first_name"] == "ANNIE"
    assert data["sex"] == "F"

@pytest.mark.asyncio
async def test_create_student_conflict(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    exam_id = str(uuid6())
    student_data = {
        "student_global_id": str(uuid6()),
        "exam_id": exam_id,
        "student_id": "S1869-0001",
        "centre_number": "S1869",
        "first_name": "ANNIE",
        "middle_name": "STEPHANO",
        "surname": "SIWITI",
        "sex": "F"
    }
    await client.post("/api/v1/students/", json=student_data, headers=headers)
    response = await client.post("/api/v1/students/", json={**student_data, "student_global_id": str(uuid6())}, headers=headers)
    assert response.status_code == status.HTTP_409_CONFLICT

@pytest.mark.asyncio
async def test_get_student(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    student_global_id = str(uuid6())
    student = StudentModel(
        student_global_id=student_global_id,
        exam_id=str(uuid6()),
        student_id="S1869-0001",
        centre_number="S1869",
        first_name="ANNIE",
        middle_name="STEPHANO",
        surname="SIWITI",
        sex="F"
    )
    async_session.add(student)
    await async_session.commit()
    response = await client.get(f"/api/v1/students/{student_global_id}", headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["student_id"] == "S1869-0001"

@pytest.mark.asyncio
async def test_get_student_not_found(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    response = await client.get(f"/api/v1/students/{str(uuid6())}", headers=headers)
    assert response.status_code == status.HTTP_404_NOT_FOUND

@pytest.mark.asyncio
async def test_get_students(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    exam_id = str(uuid6())
    students = [
        StudentModel(
            student_global_id=str(uuid6()),
            exam_id=exam_id,
            student_id="S1869-0001",
            centre_number="S1869",
            first_name="ANNIE",
            middle_name="STEPHANO",
            surname="SIWITI",
            sex="F"
        ),
        StudentModel(
            student_global_id=str(uuid6()),
            exam_id=exam_id,
            student_id="S1869-0002",
            centre_number="S1869",
            first_name="MOUSA",
            middle_name="COUNTE",
            surname="BALLA",
            sex="M"
        )
    ]
    async_session.add_all(students)
    await async_session.commit()
    response = await client.get("/api/v1/students/", headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data) == 2
    assert data[0]["student_id"] == "S1869-0001"

@pytest.mark.asyncio
async def test_create_student_unauthorized(client):
    student_data = {
        "student_global_id": str(uuid6()),
        "exam_id": str(uuid6()),
        "student_id": "S1869-0001",
        "centre_number": "S1869",
        "first_name": "ANNIE",
        "middle_name": "STEPHANO",
        "surname": "SIWITI",
        "sex": "F"
    }
    response = await client.post("/api/v1/students/", json=student_data)
    assert response.status_code == status.HTTP_401_UNAUTHORIZED
