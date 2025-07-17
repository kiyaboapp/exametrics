from pydantic import BaseModel
from typing import Optional

class CouncilBase(BaseModel):
    council_name: str
    region_id: int

class CouncilCreate(CouncilBase):
    pass

class CouncilInDB(CouncilBase):
    council_id: int

    class Config:
        from_attributes = True