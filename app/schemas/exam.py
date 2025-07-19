from pydantic import BaseModel
from datetime import date
from typing import Optional
from uuid import UUID
from app.db.models.exam import AvgStyle, ExamLevel
from pydantic import ConfigDict

class ExamBase(BaseModel):
    exam_name: str
    exam_name_swahili: Optional[str] = None
    start_date: date
    end_date: date
    avg_style: AvgStyle
    exam_level: ExamLevel
    board_id: UUID

class ExamCreate(ExamBase):
    pass

class Exam(ExamBase):
    exam_id: UUID
    model_config = ConfigDict(from_attributes=True)