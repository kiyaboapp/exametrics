# app/api/v1/student_subjects.py
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi import HTTPException, status
from app.db.database import get_db
from app.db.models.student_subject import StudentSubject
from app.schemas.student_subject import StudentSubjectCreate, StudentSubject
import uuid6


router = APIRouter()

@router.post("/", response_model=StudentSubject)
async def create_new_student_subject(student_subject: StudentSubjectCreate, db: AsyncSession = Depends(get_db)):
    existing_subject = await db.execute(select(StudentSubject).filter(
        StudentSubject.exam_id == student_subject.exam_id,
        StudentSubject.student_global_id == student_subject.student_global_id,
        StudentSubject.subject_code == student_subject.subject_code,
        StudentSubject.centre_number == student_subject.centre_number
    ))
    if existing_subject.scalars().first():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Student subject already exists for this exam, student, and subject"
        )
    
    db_student_subject = StudentSubject(**student_subject.dict(), id=str(uuid6()))
    db.add(db_student_subject)
    await db.commit()
    await db.refresh(db_student_subject)
    return StudentSubject.from_orm(db_student_subject)

@router.get("/{exam_id}/{student_global_id}/{subject_code}", response_model=StudentSubject)
async def get_student_subject(exam_id: str, student_global_id: str, subject_code: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(StudentSubject).filter(
        StudentSubject.exam_id == exam_id,
        StudentSubject.student_global_id == student_global_id,
        StudentSubject.subject_code == subject_code
    ))
    student_subject = result.scalars().first()
    if not student_subject:
        raise HTTPException(status_code=404, detail="Student subject not found")
    return StudentSubject.from_orm(student_subject)

@router.get("/{exam_id}", response_model=list[StudentSubject])
async def get_student_subjects_by_exam(exam_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(StudentSubject).filter(StudentSubject.exam_id == exam_id))
    return [StudentSubject.from_orm(subject) for subject in result.scalars().all()]