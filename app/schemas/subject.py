from pydantic import BaseModel
from pydantic import ConfigDict

class SubjectBase(BaseModel):
    subject_name: str
    subject_short: str
    has_practical: bool = False
    exclude_from_gpa: bool = False

class SubjectCreate(SubjectBase):
    subject_code: str

class Subject(SubjectBase):
    subject_code: str
    model_config = ConfigDict(from_attributes=True)