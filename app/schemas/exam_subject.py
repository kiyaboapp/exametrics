from pydantic import BaseModel
from typing import Optional

class ExamSubjectBase(BaseModel):
    exam_id: str
    subject_code: str
    subject_name: str
    subject_short: str
    is_present: bool = False
    has_practical: bool = False
    exclude_from_gpa: bool = False

class ExamSubjectCreate(ExamSubjectBase):
    pass

class ExamSubjectInDB(ExamSubjectBase):
    display_name: str

    class Config:
        from_attributes = True