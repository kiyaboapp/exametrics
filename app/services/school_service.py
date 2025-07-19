# app/services/school_service.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi import HTTPException, status
from app.db.models.school import School
from app.schemas.school import SchoolCreate, School

async def create_school(db: AsyncSession, school: SchoolCreate) -> School:
    # Check for existing school
    existing_school = await db.execute(select(School).filter(School.centre_number == school.centre_number))
    if existing_school.scalars().first():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="School already exists with this centre number"
        )
    
    db_school = School(**school.dict())
    db.add(db_school)
    await db.commit()
    await db.refresh(db_school)
    return School.from_orm(db_school)

async def get_school(db: AsyncSession, centre_number: str) -> School:
    result = await db.execute(select(School).filter(School.centre_number == centre_number))
    school = result.scalars().first()
    if not school:
        raise HTTPException(status_code=404, detail="School not found")
    return School.from_orm(school)

async def get_schools(db: AsyncSession, skip: int = 0, limit: int = 100) -> list[School]:
    result = await db.execute(select(School).offset(skip).limit(limit))
    return [School.from_orm(school) for school in result.scalars().all()]