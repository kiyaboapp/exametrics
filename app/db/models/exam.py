from sqlalchemy import Column, String, Date, Enum, JSON, ForeignKey
from sqlalchemy.orm import relationship
from app.db.database import Base
import enum

class AvgStyle(str, enum.Enum):
    AUTO = "auto"
    SEVEN_BEST = "seven_best"
    EIGHT_BEST = "eight_best"

class Exam(Base):
    __tablename__ = "exams"

    exam_id = Column(String(36), primary_key=True, default="uuid()")
    board_id = Column(String(36), ForeignKey("exam_boards.board_id"), nullable=False)
    exam_name = Column(String(100), nullable=False)
    exam_name_swahili = Column(String(100))
    start_date = Column(Date, nullable=False, index=True)
    end_date = Column(Date, nullable=False)
    avg_style = Column(Enum(AvgStyle), nullable=False, default=AvgStyle.AUTO)
    calculation_rules = Column(JSON)

    # Relationships
    board = relationship("ExamBoard", back_populates="exams")
    divisions = relationship("ExamDivision", back_populates="exam")
    grades = relationship("ExamGrade", back_populates="exam")
    subjects = relationship("ExamSubject", back_populates="exam")
    results = relationship("Result", back_populates="exam")
    student_subjects = relationship("StudentSubject", back_populates="exam")
    user_exams = relationship("UserExam", back_populates="exam")