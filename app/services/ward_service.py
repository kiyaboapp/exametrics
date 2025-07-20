
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi import HTTPException, status
from app.db.models.ward import Ward as WardModel
from app.db.schemas.ward import WardCreate, Ward
from uuid6 import uuid6

async def create_ward(db: AsyncSession, ward: WardCreate) -> Ward:
    existing_ward = await db.execute(select(WardModel).filter(WardModel.ward_name == ward.ward_name, WardModel.council_id == ward.council_id))
    if existing_ward.scalars().first():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Ward already exists with this name in the council"
        )
    ward_data = ward.model_dump()
    ward_data["ward_id"] = ward_data.get("ward_id", 0)  # Auto-incremented by DB
    db_ward = WardModel(**ward_data)
    db.add(db_ward)
    await db.commit()
    await db.refresh(db_ward)
    return Ward.model_validate(db_ward)

async def get_ward(db: AsyncSession, ward_id: int) -> Ward:
    result = await db.execute(select(WardModel).filter(WardModel.ward_id == ward_id))
    ward = result.scalars().first()
    if not ward:
        raise HTTPException(status_code=404, detail="Ward not found")
    return Ward.model_validate(ward)

async def get_wards(db: AsyncSession, skip: int = 0, limit: int = 100) -> list[Ward]:
    result = await db.execute(select(WardModel).offset(skip).limit(limit))
    return [Ward.model_validate(ward) for ward in result.scalars().all()]
