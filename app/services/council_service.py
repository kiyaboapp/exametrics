
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi import HTTPException, status
from app.db.models.council import Council as CouncilModel
from app.db.schemas.council import CouncilCreate, Council
from uuid6 import uuid6

async def create_council(db: AsyncSession, council: CouncilCreate) -> Council:
    existing_council = await db.execute(select(CouncilModel).filter(CouncilModel.council_name == council.council_name, CouncilModel.region_id == council.region_id))
    if existing_council.scalars().first():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Council already exists with this name in the region"
        )
    council_data = council.model_dump()
    council_data["council_id"] = council_data.get("council_id", 0)  # Auto-incremented by DB
    db_council = CouncilModel(**council_data)
    db.add(db_council)
    await db.commit()
    await db.refresh(db_council)
    return Council.model_validate(db_council)

async def get_council(db: AsyncSession, council_id: int) -> Council:
    result = await db.execute(select(CouncilModel).filter(CouncilModel.council_id == council_id))
    council = result.scalars().first()
    if not council:
        raise HTTPException(status_code=404, detail="Council not found")
    return Council.model_validate(council)

async def get_councils(db: AsyncSession, skip: int = 0, limit: int = 100) -> list[Council]:
    result = await db.execute(select(CouncilModel).offset(skip).limit(limit))
    return [Council.model_validate(council) for council in result.scalars().all()]
