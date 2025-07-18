from sqlalchemy import Column, String, Date, Enum, JSON, ForeignKey
from sqlalchemy.orm import relationship
from app.db.database import Base
import enum
from uuid6 import uuid6  # Added import for UUIDv6 generation

class AvgStyle(str, enum.Enum):
    AUTO = "auto"
    SEVEN_BEST = "seven_best"
    EIGHT_BEST = "eight_best"

class ExamLevel(str, enum.Enum):
    STNA = "stna"
    SFNA = "sfna"
    PSLE = "psle"
    FTNA = "ftna"
    CSEE = "csee"
    ACSEE = "acsee"

class Exam(Base):
    __tablename__ = "exams"

    exam_id = Column(String(36), primary_key=True, default=lambda: str(uuid6()))  # Changed to UUIDv6
    board_id = Column(String(36), ForeignKey("exam_boards.board_id"), nullable=False)
    exam_name = Column(String(100), nullable=False)
    exam_name_swahili = Column(String(100))
    start_date = Column(Date, nullable=False, index=True)
    end_date = Column(Date, nullable=False)
    avg_style = Column(Enum(AvgStyle), nullable=False, default=AvgStyle.AUTO)
    calculation_rules = Column(JSON)
    exam_level = Column(Enum(ExamLevel), nullable=False)

    # Relationships
    board = relationship("ExamBoard", back_populates="exams")
    divisions = relationship("ExamDivision", back_populates="exam")
    grades = relationship("ExamGrade", back_populates="exam")
    subjects = relationship("ExamSubject", back_populates="exam")
    results = relationship("Result", back_populates="exam")
    student_subjects = relationship("StudentSubject", back_populates="exam")
    user_exams = relationship("UserExam", back_populates="exam")