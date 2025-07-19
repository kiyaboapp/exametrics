from pydantic import BaseModel
from uuid import UUID
from typing import Optional
from pydantic import ConfigDict

class ExamSubjectBase(BaseModel):
    exam_id: UUID
    subject_code: str
    subject_name: str
    subject_short: str
    is_present: Optional[bool] = None
    has_practical: bool
    exclude_from_gpa: bool

class ExamSubjectCreate(ExamSubjectBase):
    pass

class ExamSubject(ExamSubjectBase):
    id: int
    display_name: str
    model_config = ConfigDict(from_attributes=True)