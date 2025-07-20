
import pytest
import httpx
from fastapi import status
from app.db.schemas.exam_subject import ExamSubjectCreate
from app.db.models.exam_subject import ExamSubject as ExamSubjectModel

@pytest.mark.asyncio
async def test_create_exam_subject(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    subject_data = {"id": 1, "exam_id": str(uuid6()), "subject_code": "011", "subject_name": "CIVICS", "subject_shortname": "CIV"}
    response = await client.post("/api/v1/exam-subjects/", json=subject_data, headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["subject_code"] == "011"
    assert data["subject_shortname"] == "CIV"

@pytest.mark.asyncio
async def test_create_exam_subject_conflict(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    exam_id = str(uuid6())
    subject_data = {"id": 1, "exam_id": exam_id, "subject_code": "011", "subject_name": "CIVICS", "subject_shortname": "CIV"}
    await client.post("/api/v1/exam-subjects/", json=subject_data, headers=headers)
    response = await client.post("/api/v1/exam-subjects/", json={"id": 2, "exam_id": exam_id, "subject_code": "011", "subject_name": "CIVICS", "subject_shortname": "CIV"}, headers=headers)
    assert response.status_code == status.HTTP_409_CONFLICT

@pytest.mark.asyncio
async def test_get_exam_subject(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    subject = ExamSubjectModel(id=1, exam_id=str(uuid6()), subject_code="011", subject_name="CIVICS", subject_shortname="CIV")
    async_session.add(subject)
    await async_session.commit()
    response = await client.get("/api/v1/exam-subjects/1", headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["subject_code"] == "011"

@pytest.mark.asyncio
async def test_get_exam_subject_not_found(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    response = await client.get("/api/v1/exam-subjects/999", headers=headers)
    assert response.status_code == status.HTTP_404_NOT_FOUND

@pytest.mark.asyncio
async def test_get_exam_subjects(client, async_session, login_token):
    headers = {"Authorization": f"Bearer {login_token}"}
    exam_id = str(uuid6())
    subjects = [
        ExamSubjectModel(id=1, exam_id=exam_id, subject_code="011", subject_name="CIVICS", subject_shortname="CIV"),
        ExamSubjectModel(id=2, exam_id=exam_id, subject_code="012", subject_name="HISTORY", subject_shortname="HIS")
    ]
    async_session.add_all(subjects)
    await async_session.commit()
    response = await client.get("/api/v1/exam-subjects/", headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data) == 2
    assert data[0]["subject_code"] == "011"

@pytest.mark.asyncio
async def test_create_exam_subject_unauthorized(client):
    subject_data = {"id": 1, "exam_id": str(uuid6()), "subject_code": "011", "subject_name": "CIVICS", "subject_shortname": "CIV"}
    response = await client.post("/api/v1/exam-subjects/", json=subject_data)
    assert response.status_code == status.HTTP_401_UNAUTHORIZED
