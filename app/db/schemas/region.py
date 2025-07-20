
from pydantic import BaseModel, ConfigDict

class RegionBase(BaseModel):
    region_id: int
    region_name: str

class RegionCreate(RegionBase):
    pass

class Region(RegionBase):
    model_config = ConfigDict(from_attributes=True)
