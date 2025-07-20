
from sqlalchemy import Column, String, ForeignKey, Date, Enum
from sqlalchemy.orm import relationship
from app.db.database import Base
from uuid6 import uuid6
import enum

class AvgStyle(str, enum.Enum):
    AUTO = "AUTO"
    SEVEN_BEST = "SEVEN_BEST"
    EIGHT_BEST = "EIGHT_BEST"

class ExamLevel(str, enum.Enum):
    STNA = "STNA"
    SFNA = "SFNA"
    PSLE = "PSLE"
    FTNA = "FTNA"
    CSEE = "CSEE"
    ACSEE = "ACSEE"

class Exam(Base):
    __tablename__ = "exams"
    exam_id = Column(String(36), primary_key=True, default=lambda: str(uuid6()))
    board_id = Column(String(36), ForeignKey("exam_boards.board_id", onupdate="CASCADE"), nullable=False)
    exam_name = Column(String(100), nullable=False)
    exam_name_swahili = Column(String(100))
    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=False)
    avg_style = Column(Enum(AvgStyle), nullable=False)  # MySQL: Native ENUM
    exam_level = Column(Enum(ExamLevel), nullable=False)  # MySQL: Native ENUM
    board = relationship("ExamBoard", back_populates="exams")
    exam_subjects = relationship("ExamSubject", back_populates="exam")
    exam_divisions = relationship("ExamDivision", back_populates="exam")
    exam_grades = relationship("ExamGrade", back_populates="exam")
    students = relationship("Student", back_populates="exam")
    results = relationship("Result", back_populates="exam")
    user_exams = relationship("UserExam", back_populates="exam")
