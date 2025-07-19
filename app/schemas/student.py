from pydantic import BaseModel
from typing import Optional
from uuid import UUID
from app.db.models.student import Sex
from pydantic import ConfigDict

class StudentBase(BaseModel):
    student_id: str
    centre_number: str
    first_name: str
    middle_name: Optional[str] = None
    surname: str
    sex: Sex
    exam_id: UUID

class StudentCreate(StudentBase):
    student_global_id:Optional[UUID]

class Student(StudentBase):
    student_global_id: UUID
    full_name: str
    model_config = ConfigDict(from_attributes=True)