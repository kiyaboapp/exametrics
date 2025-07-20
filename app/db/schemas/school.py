
from typing import Optional
from pydantic import BaseModel, ConfigDict
from app.db.models.school import SchoolType

class SchoolBase(BaseModel):
    centre_number: str
    school_name: str
    region_id: Optional[int]=None
    council_id: Optional[int]=None
    ward_id: Optional[int]=None
    region_name: Optional[str]=None
    council_name: Optional[str]=None
    ward_name: Optional[str]=None
    school_type: SchoolType

class SchoolCreate(SchoolBase):
    pass

class School(SchoolBase):
    model_config = ConfigDict(from_attributes=True)
