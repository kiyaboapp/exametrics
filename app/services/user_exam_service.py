
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi import HTTPException, status
from app.db.models.user_exam import UserExam as UserExamModel
from app.db.schemas.user_exam import UserExamCreate, UserExam

async def create_user_exam(db: AsyncSession, user_exam: UserExamCreate) -> UserExam:
    existing_user_exam = await db.execute(select(UserExamModel).filter(UserExamModel.user_id == user_exam.user_id, UserExamModel.exam_id == user_exam.exam_id))
    if existing_user_exam.scalars().first():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="User exam already exists for this user and exam"
        )
    user_exam_data = user_exam.model_dump()
    db_user_exam = UserExamModel(**user_exam_data)
    db.add(db_user_exam)
    await db.commit()
    await db.refresh(db_user_exam)
    return UserExam.model_validate(db_user_exam)

async def get_user_exam(db: AsyncSession, user_id: str, exam_id: str) -> UserExam:
    result = await db.execute(select(UserExamModel).filter(UserExamModel.user_id == user_id, UserExamModel.exam_id == exam_id))
    user_exam = result.scalars().first()
    if not user_exam:
        raise HTTPException(status_code=404, detail="User exam not found")
    return UserExam.model_validate(user_exam)

async def get_user_exams(db: AsyncSession, skip: int = 0, limit: int = 100) -> list[UserExam]:
    result = await db.execute(select(UserExamModel).offset(skip).limit(limit))
    return [UserExam.model_validate(user_exam) for user_exam in result.scalars().all()]
