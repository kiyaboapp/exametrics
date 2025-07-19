# app/services/exam_grade_service.py

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi import HTTPException, status
from app.db.models.exam_grade import ExamGrade
from app.schemas.exam_grade import ExamGradeCreate, ExamGrade

async def create_exam_grade(db: AsyncSession, grade: ExamGradeCreate) -> ExamGrade:
    # Validate grade points and values
    if grade.lower_value > grade.highest_value:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="lower_value must be less than or equal to highest_value"
        )
    
    existing_grade = await db.execute(select(ExamGrade).filter(
        ExamGrade.exam_id == grade.exam_id,
        ExamGrade.grade == grade.grade
    ))
    if existing_grade.scalars().first():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Grade already exists for this exam"
        )
    
    db_grade = ExamGrade(**grade.dict())
    db.add(db_grade)
    await db.commit()
    await db.refresh(db_grade)
    return ExamGrade.from_orm(db_grade)

async def get_exam_grade(db: AsyncSession, exam_id: str, grade: str) -> ExamGrade:
    result = await db.execute(select(ExamGrade).filter(
        ExamGrade.exam_id == exam_id,
        ExamGrade.grade == grade
    ))
    grade = result.scalars().first()
    if not grade:
        raise HTTPException(status_code=404, detail="Exam grade not found")
    return ExamGrade.from_orm(grade)

async def get_exam_grades(db: AsyncSession, exam_id: str) -> list[ExamGrade]:
    result = await db.execute(select(ExamGrade).filter(ExamGrade.exam_id == exam_id))
    return [ExamGrade.from_orm(grade) for grade in result.scalars().all()]