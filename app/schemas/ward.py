from pydantic import BaseModel
from typing import Optional

class WardBase(BaseModel):
    ward_name: str
    council_id: int

class WardCreate(WardBase):
    pass

class WardInDB(WardBase):
    ward_id: int

    class Config:
        from_attributes = True