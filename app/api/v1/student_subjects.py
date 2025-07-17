from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.db.database import get_db
from app.db.models.student_subject import StudentSubject
from app.schemas.student_subject import StudentSubjectCreate, StudentSubjectInDB

router = APIRouter()

@router.post("/", response_model=StudentSubjectInDB)
async def create_new_student_subject(student_subject: StudentSubjectCreate, db: AsyncSession = Depends(get_db)):
    db_student_subject = StudentSubject(**student_subject.dict())
    db.add(db_student_subject)
    await db.commit()
    await db.refresh(db_student_subject)
    return StudentSubjectInDB.from_orm(db_student_subject)

@router.get("/{exam_id}/{student_id}/{subject_code}", response_model=StudentSubjectInDB)
async def get_student_subject(exam_id: str, student_id: str, subject_code: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(StudentSubject).filter(
            StudentSubject.exam_id == exam_id,
            StudentSubject.student_id == student_id,
            StudentSubject.subject_code == subject_code
        )
    )
    student_subject = result.scalars().first()
    if not student_subject:
        raise HTTPException(status_code=404, detail="Student subject not found")
    return StudentSubjectInDB.from_orm(student_subject)