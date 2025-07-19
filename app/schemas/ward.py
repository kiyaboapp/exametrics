from pydantic import BaseModel
from typing import Optional
from pydantic import ConfigDict

class WardBase(BaseModel):
    ward_name: str
    council_id: int

class WardCreate(WardBase):
    pass

class Ward(WardBase):
    ward_id: int
    model_config = ConfigDict(from_attributes=True)


class WardInDB(WardBase):
    ward_id: int

    class Config:
        from_attributes = True