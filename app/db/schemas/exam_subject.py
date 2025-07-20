
from pydantic import BaseModel, ConfigDict

class ExamSubjectBase(BaseModel):
    exam_id: str
    subject_code: str
    subject_name: str
    subject_short: str
    has_practical: bool
    exclude_from_gpa: bool

class ExamSubjectCreate(ExamSubjectBase):
    pass

class ExamSubject(ExamSubjectBase):
    id: int
    model_config = ConfigDict(from_attributes=True)
