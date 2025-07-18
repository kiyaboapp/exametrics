# app/schemas/exam.py
from pydantic import BaseModel
from typing import Optional, Dict
from datetime import date
from enum import Enum

class AvgStyle(str, Enum):
    AUTO = "auto"
    SEVEN_BEST = "seven_best"
    EIGHT_BEST = "eight_best"

class ExamLevel(str, Enum):
    STNA = "stna"
    SFNA = "sfna"
    PSLE = "psle"
    FTNA = "ftna"
    CSEE = "csee"
    ACSEE = "acsee"

class ExamBase(BaseModel):
    board_id: str
    exam_name: str
    exam_name_swahili: Optional[str] = None
    start_date: date
    end_date: date
    exam_level: ExamLevel
    avg_style: AvgStyle = AvgStyle.AUTO
    calculation_rules: Optional[Dict] = None

class ExamCreate(ExamBase):
    pass

class ExamInDB(ExamBase):
    exam_id: str

    class Config:
        from_attributes = True