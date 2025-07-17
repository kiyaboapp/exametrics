from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi import Depends, HTTPException, status
from app.db.models.student import Student
from app.schemas.student import StudentCreate, StudentInDB
from app.db.database import get_db

async def create_student(db: AsyncSession, student: StudentCreate) -> StudentInDB:
    db_student = Student(**student.dict())
    db.add(db_student)
    await db.commit()
    await db.refresh(db_student)
    return StudentInDB.from_orm(db_student)

async def get_student(db: AsyncSession, student_global_id: str) -> StudentInDB:
    result = await db.execute(select(Student).filter(Student.student_global_id == student_global_id))
    student = result.scalars().first()
    if not student:
        raise HTTPException(status_code=404, detail="Student not found")
    return StudentInDB.from_orm(student)