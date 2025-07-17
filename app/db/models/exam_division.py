from sqlalchemy import Column, String, Integer, ForeignKey
from sqlalchemy.orm import relationship
from app.db.database import Base

class ExamDivision(Base):
    __tablename__ = "exam_divisions"

    exam_id = Column(String(36), ForeignKey("exams.exam_id"), primary_key=True)
    division = Column(String(3), primary_key=True)
    lowest_points = Column(Integer, nullable=False)
    highest_points = Column(Integer, nullable=False)

    # Relationship
    exam = relationship("Exam", back_populates="divisions")