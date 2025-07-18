# app/services/exam_grade_service.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi import HTTPException
from app.db.models.exam_grade import ExamGrade
from app.schemas.exam_grade import ExamGradeCreate, ExamGradeInDB

async def create_exam_grade(db: AsyncSession, grade: ExamGradeCreate) -> ExamGradeInDB:
    db_grade = ExamGrade(**grade.dict())
    db.add(db_grade)
    await db.commit()
    await db.refresh(db_grade)
    return ExamGradeInDB.from_orm(db_grade)

async def get_exam_grade(db: AsyncSession, exam_id: str, grade: str) -> ExamGradeInDB:
    result = await db.execute(
        select(ExamGrade).filter(
            ExamGrade.exam_id == exam_id,
            ExamGrade.grade == grade
        )
    )
    grade = result.scalars().first()
    if not grade:
        raise HTTPException(status_code=404, detail="Exam grade not found")
    return ExamGradeInDB.from_orm(grade)