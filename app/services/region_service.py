
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi import HTTPException, status
from app.db.models.region import Region as RegionModel
from app.db.schemas.region import RegionCreate, Region
from uuid6 import uuid6

async def create_region(db: AsyncSession, region: RegionCreate) -> Region:
    existing_region = await db.execute(select(RegionModel).filter(RegionModel.region_name == region.region_name))
    if existing_region.scalars().first():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Region already exists with this name"
        )
    region_data = region.model_dump()
    region_data["region_id"] = region_data.get("region_id", 0)  # Auto-incremented by DB
    db_region = RegionModel(**region_data)
    db.add(db_region)
    await db.commit()
    await db.refresh(db_region)
    return Region.model_validate(db_region)

async def get_region(db: AsyncSession, region_id: int) -> Region:
    result = await db.execute(select(RegionModel).filter(RegionModel.region_id == region_id))
    region = result.scalars().first()
    if not region:
        raise HTTPException(status_code=404, detail="Region not found")
    return Region.model_validate(region)

async def get_regions(db: AsyncSession, skip: int = 0, limit: int = 100) -> list[Region]:
    result = await db.execute(select(RegionModel).offset(skip).limit(limit))
    return [Region.model_validate(region) for region in result.scalars().all()]
