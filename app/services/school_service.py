
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi import HTTPException, status
from app.db.models.school import School as SchoolModel
from app.db.schemas.school import SchoolCreate, School
from uuid6 import uuid6

async def create_school(db: AsyncSession, school: SchoolCreate) -> School:
    existing_school = await db.execute(select(SchoolModel).filter(SchoolModel.centre_number == school.centre_number))
    if existing_school.scalars().first():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="School already exists with this centre number"
        )
    school_data = school.model_dump()
    db_school = SchoolModel(**school_data)
    db.add(db_school)
    await db.commit()
    await db.refresh(db_school)
    return School.model_validate(db_school)

async def get_school(db: AsyncSession, centre_number: str) -> School:
    result = await db.execute(select(SchoolModel).filter(SchoolModel.centre_number == centre_number))
    school = result.scalars().first()
    if not school:
        raise HTTPException(status_code=404, detail="School not found")
    return School.model_validate(school)

async def get_schools(db: AsyncSession, skip: int = 0, limit: int = 100) -> list[School]:
    result = await db.execute(select(SchoolModel).offset(skip).limit(limit))
    return [School.model_validate(school) for school in result.scalars().all()]
