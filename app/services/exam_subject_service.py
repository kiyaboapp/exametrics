from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi import HTTPException, status
from app.db.models.exam_subject import ExamSubject
from app.schemas.exam_subject import ExamSubjectCreate, ExamSubject

async def create_exam_subject(db: AsyncSession, exam_subject: ExamSubjectCreate) -> ExamSubject:
    existing_subject = await db.execute(select(ExamSubject).filter(
        ExamSubject.exam_id == exam_subject.exam_id,
        ExamSubject.subject_code == exam_subject.subject_code
    ))
    if existing_subject.scalars().first():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Subject already exists for this exam"
        )
    
    db_subject = ExamSubject(**exam_subject.dict())
    db.add(db_subject)
    await db.commit()
    await db.refresh(db_subject)
    return ExamSubject.from_orm(db_subject)

async def get_exam_subject(db: AsyncSession, exam_id: str, subject_code: str) -> ExamSubject:
    result = await db.execute(select(ExamSubject).filter(
        ExamSubject.exam_id == exam_id,
        ExamSubject.subject_code == subject_code
    ))
    exam_subject = result.scalars().first()
    if not exam_subject:
        raise HTTPException(status_code=404, detail="Exam subject not found")
    return ExamSubject.from_orm(exam_subject)

async def get_exam_subjects(db: AsyncSession, exam_id: str) -> list[ExamSubject]:
    result = await db.execute(select(ExamSubject).filter(ExamSubject.exam_id == exam_id))
    return [ExamSubject.from_orm(subject) for subject in result.scalars().all()]