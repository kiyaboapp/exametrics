from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi import Depends, HTTPException, status
from app.db.models.school import School
from app.schemas.school import SchoolCreate, SchoolInDB
from app.db.database import get_db

async def create_school(db: AsyncSession, school: SchoolCreate) -> SchoolInDB:
    db_school = School(**school.dict())
    db.add(db_school)
    await db.commit()
    await db.refresh(db_school)
    return SchoolInDB.from_orm(db_school)

async def get_school(db: AsyncSession, centre_number: str) -> SchoolInDB:
    result = await db.execute(select(School).filter(School.centre_number == centre_number))
    school = result.scalars().first()
    if not school:
        raise HTTPException(status_code=404, detail="School not found")
    return SchoolInDB.from_orm(school)