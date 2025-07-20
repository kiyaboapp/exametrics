
from pydantic import BaseModel, ConfigDict

class SubjectBase(BaseModel):
    subject_code: str
    subject_name: str
    subject_short: str
    has_practical: bool
    exclude_from_gpa: bool

class SubjectCreate(SubjectBase):
    pass

class Subject(SubjectBase):
    model_config = ConfigDict(from_attributes=True)
