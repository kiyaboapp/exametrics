from pydantic import BaseModel

class ExamDivisionBase(BaseModel):
    exam_id: str
    division: str
    lowest_points: int
    highest_points: int

class ExamDivisionCreate(ExamDivisionBase):
    pass

class ExamDivisionInDB(ExamDivisionBase):
    class Config:
        from_attributes = True