from sqlalchemy import Column, String, Float, Integer, ForeignKey
from sqlalchemy.orm import relationship
from app.db.database import Base

class ExamGrade(Base):
    __tablename__ = "exam_grades"

    exam_id = Column(String(36), ForeignKey("exams.exam_id"), primary_key=True)
    grade = Column(String(2), primary_key=True)
    lower_value = Column(Float, nullable=False)
    highest_value = Column(Float, nullable=False)
    grade_points = Column(Float, nullable=False)
    division_points = Column(Integer, nullable=False)

    # Relationship
    exam = relationship("Exam", back_populates="grades")