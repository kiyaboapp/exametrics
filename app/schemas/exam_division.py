from pydantic import BaseModel
from uuid import UUID
from pydantic import ConfigDict

class ExamDivisionBase(BaseModel):
    exam_id: UUID
    division: str
    lowest_points: int
    highest_points: int

class ExamDivisionCreate(ExamDivisionBase):
    pass

class ExamDivision(ExamDivisionBase):
    id: int

    model_config = ConfigDict(from_attributes=True)