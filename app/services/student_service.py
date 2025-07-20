
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi import HTTPException, status
from app.db.models.student import Student as StudentModel
from app.db.schemas.student import StudentCreate, Student
from uuid6 import uuid6

async def create_student(db: AsyncSession, student: StudentCreate) -> Student:
    existing_student = await db.execute(select(StudentModel).filter(StudentModel.exam_id == student.exam_id, StudentModel.student_id == student.student_id, StudentModel.centre_number == student.centre_number))
    if existing_student.scalars().first():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Student already exists with this exam ID, student ID, and centre number"
        )
    student_data = student.model_dump()
    student_data["student_global_id"] = str(uuid6())
    db_student = StudentModel(**student_data)
    db.add(db_student)
    await db.commit()
    await db.refresh(db_student)
    return Student.model_validate(db_student)

async def get_student(db: AsyncSession, student_global_id: str) -> Student:
    result = await db.execute(select(StudentModel).filter(StudentModel.student_global_id == student_global_id))
    student = result.scalars().first()
    if not student:
        raise HTTPException(status_code=404, detail="Student not found")
    return Student.model_validate(student)

async def get_students(db: AsyncSession, skip: int = 0, limit: int = 100) -> list[Student]:
    result = await db.execute(select(StudentModel).offset(skip).limit(limit))
    return [Student.model_validate(student) for student in result.scalars().all()]
