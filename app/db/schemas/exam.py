
from typing import Optional
from pydantic import BaseModel, ConfigDict
from datetime import date
from app.db.models.exam import AvgStyle, ExamLevel

class ExamBase(BaseModel):
    exam_id: str
    board_id: str
    exam_name: str
    exam_name_swahili: Optional[str]=None
    start_date: date
    end_date: date
    avg_style: AvgStyle
    exam_level: ExamLevel

class ExamCreate(ExamBase):
    pass

class Exam(ExamBase):
    model_config = ConfigDict(from_attributes=True)
