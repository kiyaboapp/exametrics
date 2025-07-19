from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi import HTTPException, status
from app.db.models.user_exam import UserExam
from app.schemas.user_exam import UserExamCreate, UserExam

async def create_user_exam(db: AsyncSession, user_exam: UserExamCreate) -> UserExam:
    existing_user_exam = await db.execute(select(UserExam).filter(
        UserExam.user_id == user_exam.user_id,
        UserExam.exam_id == user_exam.exam_id
    ))
    if existing_user_exam.scalars().first():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="User-exam assignment already exists"
        )
    
    db_user_exam = UserExam(**user_exam.dict())
    db.add(db_user_exam)
    await db.commit()
    await db.refresh(db_user_exam)
    return UserExam.from_orm(db_user_exam)

async def get_user_exam(db: AsyncSession, user_id: str, exam_id: str) -> UserExam:
    result = await db.execute(select(UserExam).filter(
        UserExam.user_id == user_id,
        UserExam.exam_id == exam_id
    ))
    user_exam = result.scalars().first()
    if not user_exam:
        raise HTTPException(status_code=404, detail="User-exam assignment not found")
    return UserExam.from_orm(user_exam)

async def get_user_exams(db: AsyncSession, user_id: str) -> list[UserExam]:
    result = await db.execute(select(UserExam).filter(UserExam.user_id == user_id))
    return [UserExam.from_orm(user_exam) for user_exam in result.scalars().all()]