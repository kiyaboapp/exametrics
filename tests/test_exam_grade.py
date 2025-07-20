
import pytest
import httpx
from fastapi import status
from app.db.schemas.exam_grade import ExamGradeCreate
from app.db.models.exam_grade import ExamGrade as ExamGradeModel
from uuid6 import uuid6

@pytest.mark.asyncio
async def test_create_exam_grade(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    grade_data = {"id": 1, "exam_id": str(uuid6()), "grade": "A", "lower_value": 75.0, "highest_value": 100.0, "grade_points": 1.0, "division_points": 1}
    response = await client.post("/api/v1/exam-grades/", json=grade_data, headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["grade"] == "A"

@pytest.mark.asyncio
async def test_create_exam_grade_conflict(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    exam_id = str(uuid6())
    grade_data = {"id": 1, "exam_id": exam_id, "grade": "A", "lower_value": 75.0, "highest_value": 100.0, "grade_points": 1.0, "division_points": 1}
    await client.post("/api/v1/exam-grades/", json=grade_data, headers=headers)
    response = await client.post("/api/v1/exam-grades/", json={"id": 2, "exam_id": exam_id, "grade": "A", "lower_value": 75.0, "highest_value": 100.0, "grade_points": 1.0, "division_points": 1}, headers=headers)
    assert response.status_code == status.HTTP_409_CONFLICT

@pytest.mark.asyncio
async def test_get_exam_grade(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    grade = ExamGradeModel(id=1, exam_id=str(uuid6()), grade="A", lower_value=75.0, highest_value=100.0, grade_points=1.0, division_points=1)
    async_session.add(grade)
    await async_session.commit()
    response = await client.get("/api/v1/exam-grades/1", headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["grade"] == "A"

@pytest.mark.asyncio
async def test_get_exam_grade_not_found(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    response = await client.get("/api/v1/exam-grades/999", headers=headers)
    assert response.status_code == status.HTTP_404_NOT_FOUND

@pytest.mark.asyncio
async def test_get_exam_grades(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    exam_id = str(uuid6())
    grades = [
        ExamGradeModel(id=1, exam_id=exam_id, grade="A", lower_value=75.0, highest_value=100.0, grade_points=1.0, division_points=1),
        ExamGradeModel(id=2, exam_id=exam_id, grade="B", lower_value=65.0, highest_value=74.9, grade_points=2.0, division_points=2)
    ]
    async_session.add_all(grades)
    await async_session.commit()
    response = await client.get("/api/v1/exam-grades/", headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data) == 2
    assert data[0]["grade"] == "A"

@pytest.mark.asyncio
async def test_create_exam_grade_unauthorized(client):
    grade_data = {"id": 1, "exam_id": str(uuid6()), "grade": "A", "lower_value": 75.0, "highest_value": 100.0, "grade_points": 1.0, "division_points": 1}
    response = await client.post("/api/v1/exam-grades/", json=grade_data)
    assert response.status_code == status.HTTP_401_UNAUTHORIZED
