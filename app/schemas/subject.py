from pydantic import BaseModel
from typing import Optional

class SubjectBase(BaseModel):
    subject_code: str
    subject_name: str
    subject_short: str
    is_present: bool = False
    has_practical: bool = False
    exclude_from_gpa: bool = False

class SubjectCreate(SubjectBase):
    pass

class SubjectInDB(SubjectBase):
    display_name: str

    class Config:
        from_attributes = True