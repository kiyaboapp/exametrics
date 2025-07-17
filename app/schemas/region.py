from pydantic import BaseModel
from typing import Optional

class RegionBase(BaseModel):
    region_name: str

class RegionCreate(RegionBase):
    pass

class RegionInDB(RegionBase):
    region_id: int

    class Config:
        from_attributes = True