from sqlalchemy import Column, Integer, String, Float, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship
from app.db.database import Base

class ExamGrade(Base):
    __tablename__ = "exam_grades"
    id = Column(Integer, primary_key=True, autoincrement=True)
    exam_id = Column(String(36), ForeignKey("exams.exam_id", ondelete="CASCADE"))
    grade = Column(String(2), nullable=False)
    lower_value = Column(Float, nullable=False)
    highest_value = Column(Float, nullable=False)
    grade_points = Column(Float, nullable=False)
    division_points = Column(Integer, nullable=False)
    
    # Relationships
    exam = relationship("Exam", back_populates="exam_grades")
    
    __table_args__ = (
        UniqueConstraint('exam_id', 'grade'),
    )