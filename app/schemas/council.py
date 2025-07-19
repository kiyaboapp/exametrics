from pydantic import BaseModel
from typing import Optional
from pydantic import ConfigDict

class CouncilBase(BaseModel):
    council_name: str
    region_id: int

class CouncilCreate(CouncilBase):
    pass

class Council(CouncilBase):
    council_id: int
    model_config = ConfigDict(from_attributes=True)



class CouncilInDB(CouncilBase):
    council_id: int

    class Config:
        from_attributes = True