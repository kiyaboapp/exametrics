
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi import HTTPException, status
from app.db.models.exam_grade import ExamGrade as ExamGradeModel
from app.db.schemas.exam_grade import ExamGradeCreate, ExamGrade
from uuid6 import uuid6

async def create_exam_grade(db: AsyncSession, exam_grade: ExamGradeCreate) -> ExamGrade:
    existing_grade = await db.execute(select(ExamGradeModel).filter(ExamGradeModel.exam_id == exam_grade.exam_id, ExamGradeModel.grade == exam_grade.grade))
    if existing_grade.scalars().first():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Exam grade already exists for this exam and grade"
        )
    grade_data = exam_grade.model_dump()
    grade_data["id"] = grade_data.get("id", 0)  # Auto-incremented by DB
    db_grade = ExamGradeModel(**grade_data)
    db.add(db_grade)
    await db.commit()
    await db.refresh(db_grade)
    return ExamGrade.model_validate(db_grade)

async def get_exam_grade(db: AsyncSession, exam_grade_id: int) -> ExamGrade:
    result = await db.execute(select(ExamGradeModel).filter(ExamGradeModel.id == exam_grade_id))
    grade = result.scalars().first()
    if not grade:
        raise HTTPException(status_code=404, detail="Exam grade not found")
    return ExamGrade.model_validate(grade)

async def get_exam_grades(db: AsyncSession, skip: int = 0, limit: int = 100) -> list[ExamGrade]:
    result = await db.execute(select(ExamGradeModel).offset(skip).limit(limit))
    return [ExamGrade.model_validate(grade) for grade in result.scalars().all()]
