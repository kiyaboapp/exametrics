from pydantic import BaseModel
from typing import Optional
from pydantic import ConfigDict

class RegionBase(BaseModel):
    region_name: str

class RegionCreate(RegionBase):
    pass

class Region(RegionBase):
    region_id: int

    model_config = ConfigDict(from_attributes=True)

class RegionInDB(RegionBase):
    region_id: int

    class Config:
        from_attributes = True