from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi import HTTPException, status
from app.db.models.subject import Subject
from app.schemas.subject import SubjectCreate, Subject

async def create_subject(db: AsyncSession, subject: SubjectCreate) -> Subject:
    existing_subject = await db.execute(select(Subject).filter(Subject.subject_code == subject.subject_code))
    if existing_subject.scalars().first():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Subject already exists with this subject code"
        )
    
    db_subject = Subject(**subject.dict())
    db.add(db_subject)
    await db.commit()
    await db.refresh(db_subject)
    return Subject.from_orm(db_subject)

async def get_subject(db: AsyncSession, subject_code: str) -> Subject:
    result = await db.execute(select(Subject).filter(Subject.subject_code == subject_code))
    subject = result.scalars().first()
    if not subject:
        raise HTTPException(status_code=404, detail="Subject not found")
    return Subject.from_orm(subject)

async def get_subjects(db: AsyncSession, skip: int = 0, limit: int = 100) -> list[Subject]:
    result = await db.execute(select(Subject).offset(skip).limit(limit))
    return [Subject.from_orm(subject) for subject in result.scalars().all()]