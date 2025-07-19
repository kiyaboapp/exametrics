# app/services/exam_service.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi import HTTPException, status
from app.db.models.exam import Exam, ExamLevel
from app.db.models.exam_division import ExamDivision
from app.db.models.exam_grade import ExamGrade
from app.schemas.exam import ExamCreate, Exam
from app.schemas.exam_division import ExamDivisionCreate
from app.schemas.exam_grade import ExamGradeCreate
from uuid6 import uuid6

async def create_exam(db: AsyncSession, exam: ExamCreate) -> Exam:
    # Check for existing exam
    existing_exam = await db.execute(select(Exam).filter(Exam.exam_name == exam.exam_name, Exam.board_id == exam.board_id))
    if existing_exam.scalars().first():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Exam already exists with this name and board"
        )
    
    exam_data = exam.dict()
    exam_data["exam_id"] = str(uuid6())
    db_exam = Exam(**exam_data)
    db.add(db_exam)
    await db.commit()
    await db.refresh(db_exam)
    
    # Add grades and divisions for FTNA, CSEE, ACSEE
    if exam.exam_level in [ExamLevel.FTNA, ExamLevel.CSEE]:
        divisions = [
            ExamDivisionCreate(exam_id=db_exam.exam_id, division="I", lowest_points=7, highest_points=17),
            ExamDivisionCreate(exam_id=db_exam.exam_id, division="II", lowest_points=18, highest_points=22),
            ExamDivisionCreate(exam_id=db_exam.exam_id, division="III", lowest_points=23, highest_points=25),
            ExamDivisionCreate(exam_id=db_exam.exam_id, division="IV", lowest_points=26, highest_points=33),
            ExamDivisionCreate(exam_id=db_exam.exam_id, division="0", lowest_points=34, highest_points=35),
        ]
        grades = [
            ExamGradeCreate(exam_id=db_exam.exam_id, grade="A", lower_value=75.0, highest_value=100.0, grade_points=1.0, division_points=1),
            ExamGradeCreate(exam_id=db_exam.exam_id, grade="B", lower_value=65.0, highest_value=74.9, grade_points=2.0, division_points=2),
            ExamGradeCreate(exam_id=db_exam.exam_id, grade="C", lower_value=45.0, highest_value=64.9, grade_points=3.0, division_points=3),
            ExamGradeCreate(exam_id=db_exam.exam_id, grade="D", lower_value=30.0, highest_value=44.9, grade_points=4.0, division_points=4),
            ExamGradeCreate(exam_id=db_exam.exam_id, grade="F", lower_value=0.0, highest_value=29.9, grade_points=5.0, division_points=5),
        ]
    elif exam.exam_level == ExamLevel.ACSEE:
        divisions = [
            ExamDivisionCreate(exam_id=db_exam.exam_id, division="I", lowest_points=3, highest_points=9),
            ExamDivisionCreate(exam_id=db_exam.exam_id, division="II", lowest_points=10, highest_points=12),
            ExamDivisionCreate(exam_id=db_exam.exam_id, division="III", lowest_points=13, highest_points=17),
            ExamDivisionCreate(exam_id=db_exam.exam_id, division="IV", lowest_points=18, highest_points=19),
            ExamDivisionCreate(exam_id=db_exam.exam_id, division="0", lowest_points=20, highest_points=21),
        ]
        grades = [
            ExamGradeCreate(exam_id=db_exam.exam_id, grade="A", lower_value=80.0, highest_value=100.0, grade_points=1.0, division_points=1),
            ExamGradeCreate(exam_id=db_exam.exam_id, grade="B", lower_value=70.0, highest_value=79.9, grade_points=2.0, division_points=2),
            ExamGradeCreate(exam_id=db_exam.exam_id, grade="C", lower_value=60.0, highest_value=69.9, grade_points=3.0, division_points=2),
            ExamGradeCreate(exam_id=db_exam.exam_id, grade="D", lower_value=50.0, highest_value=59.9, grade_points=4.0, division_points=3),
            ExamGradeCreate(exam_id=db_exam.exam_id, grade="E", lower_value=40.0, highest_value=49.9, grade_points=5.0, division_points=4),
            ExamGradeCreate(exam_id=db_exam.exam_id, grade="S", lower_value=35.0, highest_value=39.9, grade_points=6.0, division_points=4),
            ExamGradeCreate(exam_id=db_exam.exam_id, grade="F", lower_value=0.0, highest_value=34.9, grade_points=7.0, division_points=5),
        ]
    else:
        divisions = []
        grades = []

    for division in divisions:
        db_division = ExamDivision(**division.dict())
        db.add(db_division)
    
    for grade in grades:
        db_grade = ExamGrade(**grade.dict())
        db.add(db_grade)
    
    await db.commit()
    return Exam.from_orm(db_exam)

async def get_exam(db: AsyncSession, exam_id: str) -> Exam:
    result = await db.execute(select(Exam).filter(Exam.exam_id == exam_id))
    exam = result.scalars().first()
    if not exam:
        raise HTTPException(status_code=404, detail="Exam not found")
    return Exam.from_orm(exam)

async def get_exams(db: AsyncSession, skip: int = 0, limit: int = 100) -> list[Exam]:
    result = await db.execute(select(Exam).offset(skip).limit(limit))
    return [Exam.from_orm(exam) for exam in result.scalars().all()]