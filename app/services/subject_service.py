
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi import HTTPException, status
from app.db.models.subject import Subject as SubjectModel
from app.db.schemas.subject import SubjectCreate, Subject
from uuid6 import uuid6

async def create_subject(db: AsyncSession, subject: SubjectCreate) -> Subject:
    existing_subject = await db.execute(select(SubjectModel).filter(SubjectModel.subject_code == subject.subject_code))
    if existing_subject.scalars().first():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Subject already exists with this code"
        )
    subject_data = subject.model_dump()
    db_subject = SubjectModel(**subject_data)
    db.add(db_subject)
    await db.commit()
    await db.refresh(db_subject)
    return Subject.model_validate(db_subject)

async def get_subject(db: AsyncSession, subject_code: str) -> Subject:
    result = await db.execute(select(SubjectModel).filter(SubjectModel.subject_code == subject_code))
    subject = result.scalars().first()
    if not subject:
        raise HTTPException(status_code=404, detail="Subject not found")
    return Subject.model_validate(subject)

async def get_subjects(db: AsyncSession, skip: int = 0, limit: int = 100) -> list[Subject]:
    result = await db.execute(select(SubjectModel).offset(skip).limit(limit))
    return [Subject.model_validate(subject) for subject in result.scalars().all()]
