from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from fastapi import HTTPException
from app.db.models.school import School as SchoolModel
from app.db.schemas.school import SchoolCreate, School
from app.db.models.region import Region
from app.db.models.council import Council
from app.db.models.ward import Ward


async def resolve_location_data(db: AsyncSession, school_data: dict) -> dict:
    # Normalize names
    rn = school_data.get("region_name")
    cn = school_data.get("council_name")
    wn = school_data.get("ward_name")

    if rn: rn = rn.strip().upper()
    if cn: cn = cn.strip().upper()
    if wn: wn = wn.strip().upper()

    if wn:
        stmt = (
            select(Ward)
            .options(
                selectinload(Ward.council).selectinload(Council.region)
            )
            .filter(Ward.ward_name.ilike(wn))
        )
        wards = (await db.execute(stmt)).scalars().all()

        if not wards:
            raise HTTPException(404, detail=f"Ward '{wn}' not found")
        if len(wards) > 1:
            raise HTTPException(400, detail=f"Multiple wards named '{wn}' found. Please specify council or ward ID.")

        ward = wards[0]
        council = ward.council
        region = council.region

        school_data["ward_id"] = ward.ward_id
        school_data["council_id"] = council.council_id
        school_data["region_id"] = region.region_id
        school_data["ward_name"] = ward.ward_name
        school_data["council_name"] = council.council_name
        school_data["region_name"] = region.region_name

    elif cn:
        stmt = (
            select(Council)
            .options(selectinload(Council.region))
            .filter(Council.council_name.ilike(cn))
        )
        councils = (await db.execute(stmt)).scalars().all()

        if not councils:
            raise HTTPException(404, detail=f"Council '{cn}' not found")
        if len(councils) > 1:
            raise HTTPException(400, detail=f"Multiple councils named '{cn}' found. Please specify region.")

        council = councils[0]
        region = council.region

        school_data["council_id"] = council.council_id
        school_data["region_id"] = region.region_id
        school_data["council_name"] = council.council_name
        school_data["region_name"] = region.region_name

    elif rn:
        stmt = select(Region).filter(Region.region_name.ilike(rn))
        region = (await db.execute(stmt)).scalars().first()
        if not region:
            raise HTTPException(404, detail=f"Region '{rn}' not found")
        school_data["region_id"] = region.region_id
        school_data["region_name"] = region.region_name

    else:
        if "ward_id" in school_data:
            stmt = (
                select(Ward)
                .options(
                    selectinload(Ward.council).selectinload(Council.region)
                )
                .filter(Ward.ward_id == school_data["ward_id"])
            )
            ward = (await db.execute(stmt)).scalars().first()
            if not ward:
                raise HTTPException(404, detail="Ward ID not found")
            council = ward.council
            region = council.region

            school_data["ward_name"] = ward.ward_name
            school_data["council_id"] = council.council_id
            school_data["council_name"] = council.council_name
            school_data["region_id"] = region.region_id
            school_data["region_name"] = region.region_name

        elif "council_id" in school_data:
            stmt = (
                select(Council)
                .options(selectinload(Council.region))
                .filter(Council.council_id == school_data["council_id"])
            )
            council = (await db.execute(stmt)).scalars().first()
            if not council:
                raise HTTPException(404, detail="Council ID not found")
            region = council.region

            school_data["council_name"] = council.council_name
            school_data["region_id"] = region.region_id
            school_data["region_name"] = region.region_name

        elif "region_id" in school_data:
            region = await db.get(Region, school_data["region_id"])
            if not region:
                raise HTTPException(404, detail="Region ID not found")
            school_data["region_name"] = region.region_name

    return school_data


async def create_school(db: AsyncSession, school: SchoolCreate) -> School:
    result = await db.execute(select(SchoolModel).filter(SchoolModel.centre_number == school.centre_number))
    if result.scalars().first():
        raise HTTPException(status_code=409, detail="School already exists with this centre number")

    school_data = school.model_dump()
    school_data = await resolve_location_data(db, school_data)

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
