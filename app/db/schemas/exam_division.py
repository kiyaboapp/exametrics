
from pydantic import BaseModel, ConfigDict

class ExamDivisionBase(BaseModel):
    id: int
    exam_id: str
    division: str
    lowest_points: int
    highest_points: int

class ExamDivisionCreate(ExamDivisionBase):
    pass

class ExamDivision(ExamDivisionBase):
    model_config = ConfigDict(from_attributes=True)
