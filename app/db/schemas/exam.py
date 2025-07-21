
from typing import Optional
from pydantic import BaseModel, ConfigDict
from datetime import date

class ExamBase(BaseModel):
    exam_id: Optional[str]=None
    board_id: str
    exam_name: str
    exam_name_swahili: Optional[str]=None
    start_date: date
    end_date: date
    avg_style: str
    exam_level: str

class ExamCreate(ExamBase):
    pass

class Exam(ExamBase):
    model_config = ConfigDict(from_attributes=True)
