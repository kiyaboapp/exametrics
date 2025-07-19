# app/services/student_service.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi import HTTPException, status
from app.db.models.student import Student
from app.schemas.student import StudentCreate,Student
from uuid6 import uuid6

async def create_student(db: AsyncSession, student: StudentCreate) -> Student:
    # Check for existing student with composite key
    existing_student = await db.execute(select(Student).filter(
        Student.exam_id == student.exam_id,
        Student.student_id == student.student_id,
        Student.centre_number == student.centre_number
    ))
    if existing_student.scalars().first():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Student already exists with this exam ID, student ID, and centre number"
        )
    
    db_student = Student(**student.dict(), student_global_id=str(uuid6()))
    db.add(db_student)
    await db.commit()
    await db.refresh(db_student)
    return Student.from_orm(db_student)

async def get_student(db: AsyncSession, student_global_id: str) -> Student:
    result = await db.execute(select(Student).filter(Student.student_global_id == student_global_id))
    student = result.scalars().first()
    if not student:
        raise HTTPException(status_code=404, detail="Student not found")
    return Student.from_orm(student)

async def get_students(db: AsyncSession, skip: int = 0, limit: int = 100) -> list[Student]:
    result = await db.execute(select(Student).offset(skip).limit(limit))
    return [Student.from_orm(student) for student in result.scalars().all()]