from pydantic import BaseModel
from uuid import UUID
from pydantic import ConfigDict

class ExamGradeBase(BaseModel):
    exam_id: UUID
    grade: str
    lower_value: float
    highest_value: float
    grade_points: float
    division_points: int

class ExamGradeCreate(ExamGradeBase):
    pass

class ExamGrade(ExamGradeBase):
    id: int
    model_config = ConfigDict(from_attributes=True)