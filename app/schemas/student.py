from pydantic import BaseModel
from typing import Optional
from datetime import date, datetime
from enum import Enum

class Sex(str, Enum):
    MALE = "M"
    FEMALE = "F"
    OTHER = "Other"

class StudentBase(BaseModel):
    student_id: str
    first_name: str
    middle_name: Optional[str] = None
    surname: str
    sex: Sex
    centre_number: str
    date_of_birth: Optional[date] = None

class StudentCreate(StudentBase):
    pass

class StudentInDB(StudentBase):
    student_global_id: str
    full_name: str
    created_at: datetime

    class Config:
        from_attributes = True