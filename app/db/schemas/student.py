
from typing import Optional
from pydantic import BaseModel, ConfigDict
from datetime import datetime


class StudentBase(BaseModel):
    student_global_id: str
    exam_id: str
    student_id: str
    centre_number: str
    first_name: str
    middle_name: Optional[str]=None
    surname: str
    sex: str
    created_at: datetime

class StudentCreate(BaseModel):
    exam_id: str
    student_id: str
    centre_number: str
    first_name: str
    middle_name: Optional[str]=None
    surname: str
    sex: str

class Student(StudentBase):
    model_config = ConfigDict(from_attributes=True)
