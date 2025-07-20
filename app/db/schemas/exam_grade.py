
from pydantic import BaseModel, ConfigDict

class ExamGradeBase(BaseModel):
    id: int
    exam_id: str
    grade: str
    lower_value: float
    highest_value: float
    grade_points: float
    division_points: int

class ExamGradeCreate(ExamGradeBase):
    pass

class ExamGrade(ExamGradeBase):
    model_config = ConfigDict(from_attributes=True)
