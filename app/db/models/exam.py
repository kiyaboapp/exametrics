
from sqlalchemy import CheckConstraint, Column, String, ForeignKey, Date
from sqlalchemy.orm import relationship
from app.db.database import Base
from uuid6 import uuid6

class Exam(Base):
    __tablename__ = "exams"
    exam_id = Column(String(36), primary_key=True, default=lambda: str(uuid6()))
    board_id = Column(String(36), ForeignKey("exam_boards.board_id", onupdate="CASCADE"), nullable=False)
    exam_name = Column(String(100), nullable=False)
    exam_name_swahili = Column(String(100))
    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=False)
    avg_style = Column(String(20), nullable=False) 
    exam_level = Column(String(20), nullable=False)  
    board = relationship("ExamBoard", back_populates="exams")
    exam_subjects = relationship("ExamSubject", back_populates="exam")
    exam_divisions = relationship("ExamDivision", back_populates="exam")
    exam_grades = relationship("ExamGrade", back_populates="exam")
    students = relationship("Student", back_populates="exam")
    results = relationship("Result", back_populates="exam")
    user_exams = relationship("UserExam", back_populates="exam")

    __table_args__ = (
        CheckConstraint(
            "avg_style IN ('AUTO', 'SEVEN_BEST', 'EIGHT_BEST')", 
            name="valid_avg_style"
        ),
        CheckConstraint(
            "exam_level IN ('STNA', 'SFNA', 'PSLE', 'FTNA', 'CSEE', 'ACSEE')", 
            name="valid_exam_level"
        ),
    )