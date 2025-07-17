from sqlalchemy import Column, String, Boolean
from sqlalchemy.orm import relationship
from app.db.base import Base

class Subject(Base):
    __tablename__ = "subjects"

    subject_code = Column(String(10), primary_key=True)
    subject_name = Column(String(50), nullable=False)
    subject_short = Column(String(20), nullable=False)
    has_practical = Column(Boolean, nullable=False)

    # Relationships
    exam_subjects = relationship("ExamSubject", back_populates="subject")