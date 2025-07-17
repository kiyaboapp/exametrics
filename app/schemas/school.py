from pydantic import BaseModel
from typing import Optional
from enum import Enum

class SchoolType(str, Enum):
    GOVERNMENT = "government"
    PRIVATE = "private"
    UNKNOWN = "unknown"

class SchoolBase(BaseModel):
    centre_number: str
    school_name: str
    region_id: Optional[int] = None
    council_id: Optional[int] = None
    ward_id: Optional[int] = None
    school_type: SchoolType = SchoolType.UNKNOWN

class SchoolCreate(SchoolBase):
    pass

class SchoolInDB(SchoolBase):
    class Config:
        from_attributes = True