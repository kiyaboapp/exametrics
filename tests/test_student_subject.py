
import pytest
import httpx
from fastapi import status
from app.db.schemas.student_subject import StudentSubjectCreate
from app.db.models.student_subject import StudentSubject as StudentSubjectModel
from uuid6 import uuid6

@pytest.mark.asyncio
async def test_create_student_subject(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    subject_data = {
        "id": str(uuid6()),
        "exam_id": str(uuid6()),
        "student_global_id": str(uuid6()),
        "centre_number": "S1869",
        "subject_code": "011",
        "grade": "A",
        "points": 1.0
    }
    response = await client.post("/api/v1/student-subjects/", json=subject_data, headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["subject_code"] == "011"
    assert data["grade"] == "A"

@pytest.mark.asyncio
async def test_create_student_subject_conflict(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    exam_id = str(uuid6())
    student_global_id = str(uuid6())
    subject_data = {
        "id": str(uuid6()),
        "exam_id": exam_id,
        "student_global_id": student_global_id,
        "centre_number": "S1869",
        "subject_code": "011",
        "grade": "A",
        "points": 1.0
    }
    await client.post("/api/v1/student-subjects/", json=subject_data, headers=headers)
    response = await client.post("/api/v1/student-subjects/", json={**subject_data, "id": str(uuid6())}, headers=headers)
    assert response.status_code == status.HTTP_409_CONFLICT

@pytest.mark.asyncio
async def test_get_student_subject(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    subject_id = str(uuid6())
    subject = StudentSubjectModel(
        id=subject_id,
        exam_id=str(uuid6()),
        student_global_id=str(uuid6()),
        centre_number="S1869",
        subject_code="011",
        grade="A",
        points=1.0
    )
    async_session.add(subject)
    await async_session.commit()
    response = await client.get(f"/api/v1/student-subjects/{subject_id}", headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["subject_code"] == "011"

@pytest.mark.asyncio
async def test_get_student_subject_not_found(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    response = await client.get(f"/api/v1/student-subjects/{str(uuid6())}", headers=headers)
    assert response.status_code == status.HTTP_404_NOT_FOUND

@pytest.mark.asyncio
async def test_get_student_subjects(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    exam_id = str(uuid6())
    student_global_id = str(uuid6())
    subjects = [
        StudentSubjectModel(
            id=str(uuid6()),
            exam_id=exam_id,
            student_global_id=student_global_id,
            centre_number="S1869",
            subject_code="011",
            grade="A",
            points=1.0
        ),
        StudentSubjectModel(
            id=str(uuid6()),
            exam_id=exam_id,
            student_global_id=student_global_id,
            centre_number="S1869",
            subject_code="012",
            grade="B",
            points=2.0
        )
    ]
    async_session.add_all(subjects)
    await async_session.commit()
    response = await client.get("/api/v1/student-subjects/", headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data) == 2
    assert data[0]["subject_code"] == "011"

@pytest.mark.asyncio
async def test_create_student_subject_unauthorized(client):
    subject_data = {
        "id": str(uuid6()),
        "exam_id": str(uuid6()),
        "student_global_id": str(uuid6()),
        "centre_number": "S1869",
        "subject_code": "011",
        "grade": "A",
        "points": 1.0
    }
    response = await client.post("/api/v1/student-subjects/", json=subject_data)
    assert response.status_code == status.HTTP_401_UNAUTHORIZED
