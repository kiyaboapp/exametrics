from sqlalchemy import Column, Integer, String, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship
from app.db.database import Base

class ExamDivision(Base):
    __tablename__ = "exam_divisions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    exam_id = Column(String(36), ForeignKey("exams.exam_id", ondelete="CASCADE"))
    division = Column(String(3), nullable=False)
    lowest_points = Column(Integer, nullable=False)
    highest_points = Column(Integer, nullable=False)
    
    # Relationships
    exam = relationship("Exam", back_populates="exam_divisions")
    
    __table_args__ = (
        UniqueConstraint('exam_id', 'division'),
    )