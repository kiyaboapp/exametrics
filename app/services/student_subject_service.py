
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi import HTTPException, status
from app.db.models.student_subject import StudentSubject as StudentSubjectModel
from app.db.schemas.student_subject import StudentSubjectCreate, StudentSubject
from uuid6 import uuid6

async def create_student_subject(db: AsyncSession, student_subject: StudentSubjectCreate) -> StudentSubject:
    existing_subject = await db.execute(select(StudentSubjectModel).filter(
        StudentSubjectModel.exam_id == student_subject.exam_id,
        StudentSubjectModel.student_global_id == student_subject.student_global_id,
        StudentSubjectModel.centre_number == student_subject.centre_number,
        StudentSubjectModel.subject_code == student_subject.subject_code
    ))
    if existing_subject.scalars().first():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Student subject already exists for this exam, student, centre, and subject"
        )
    subject_data = student_subject.model_dump()
    subject_data["id"] = str(uuid6())
    db_subject = StudentSubjectModel(**subject_data)
    db.add(db_subject)
    await db.commit()
    await db.refresh(db_subject)
    return StudentSubject.model_validate(db_subject)

async def get_student_subject(db: AsyncSession, student_subject_id: str) -> StudentSubject:
    result = await db.execute(select(StudentSubjectModel).filter(StudentSubjectModel.id == student_subject_id))
    subject = result.scalars().first()
    if not subject:
        raise HTTPException(status_code=404, detail="Student subject not found")
    return StudentSubject.model_validate(subject)

async def get_student_subjects(db: AsyncSession, skip: int = 0, limit: int = 100) -> list[StudentSubject]:
    result = await db.execute(select(StudentSubjectModel).offset(skip).limit(limit))
    return [StudentSubject.model_validate(subject) for subject in result.scalars().all()]
