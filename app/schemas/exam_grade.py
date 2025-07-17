from pydantic import BaseModel

class ExamGradeBase(BaseModel):
    exam_id: str
    grade: str
    lower_value: float
    highest_value: float
    grade_points: float
    division_points: int

class ExamGradeCreate(ExamGradeBase):
    pass

class ExamGradeInDB(ExamGradeBase):
    class Config:
        from_attributes = True