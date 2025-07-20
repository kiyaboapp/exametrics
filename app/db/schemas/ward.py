
from pydantic import BaseModel, ConfigDict

class WardBase(BaseModel):
    ward_id: int
    ward_name: str
    council_id: int

class WardCreate(WardBase):
    pass

class Ward(WardBase):
    model_config = ConfigDict(from_attributes=True)
