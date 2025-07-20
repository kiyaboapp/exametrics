
from pydantic import BaseModel, ConfigDict

class CouncilBase(BaseModel):
    council_id: int
    council_name: str
    region_id: int

class CouncilCreate(CouncilBase):
    pass

class Council(CouncilBase):
    model_config = ConfigDict(from_attributes=True)
