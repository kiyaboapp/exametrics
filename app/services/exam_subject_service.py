
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi import HTTPException, status
from app.db.models.exam_subject import ExamSubject as ExamSubjectModel
from app.db.schemas.exam_subject import ExamSubjectCreate, ExamSubject
from uuid6 import uuid6

async def create_exam_subject(db: AsyncSession, exam_subject: ExamSubjectCreate) -> ExamSubject:
    existing_subject = await db.execute(select(ExamSubjectModel).filter(ExamSubjectModel.exam_id == exam_subject.exam_id, ExamSubjectModel.subject_code == exam_subject.subject_code))
    if existing_subject.scalars().first():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Exam subject already exists for this exam and subject code"
        )
    subject_data = exam_subject.model_dump()
    subject_data["id"] = subject_data.get("id", 0)  # Auto-incremented by DB
    db_subject = ExamSubjectModel(**subject_data)
    db.add(db_subject)
    await db.commit()
    await db.refresh(db_subject)
    return ExamSubject.model_validate(db_subject)

async def get_exam_subject(db: AsyncSession, exam_subject_id: int) -> ExamSubject:
    result = await db.execute(select(ExamSubjectModel).filter(ExamSubjectModel.id == exam_subject_id))
    subject = result.scalars().first()
    if not subject:
        raise HTTPException(status_code=404, detail="Exam subject not found")
    return ExamSubject.model_validate(subject)

async def get_exam_subjects(db: AsyncSession, skip: int = 0, limit: int = 100) -> list[ExamSubject]:
    result = await db.execute(select(ExamSubjectModel).offset(skip).limit(limit))
    return [ExamSubject.model_validate(subject) for subject in result.scalars().all()]
