from pydantic import BaseModel
from typing import Optional
from app.db.models.school import SchoolType
from pydantic import ConfigDict

class SchoolBase(BaseModel):
    school_name: str
    region_id: Optional[int] = None
    council_id: Optional[int] = None
    ward_id: Optional[int] = None
    school_type: SchoolType

class SchoolCreate(SchoolBase):
    centre_number: str

class School(SchoolBase):
    centre_number: str

    model_config = ConfigDict(from_attributes=True)