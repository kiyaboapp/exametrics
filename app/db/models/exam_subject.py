
from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship
from app.db.database import Base

class ExamSubject(Base):
    __tablename__ = "exam_subjects"
    id = Column(Integer, primary_key=True, autoincrement=True)
    exam_id = Column(String(36), ForeignKey("exams.exam_id", ondelete="CASCADE"))
    subject_code = Column(String(10), ForeignKey("subjects.subject_code", onupdate="CASCADE"))
    subject_name = Column(String(50), nullable=False)
    subject_short = Column(String(50), nullable=False)
    has_practical = Column(Boolean, nullable=False)
    exclude_from_gpa = Column(Boolean, nullable=False)
    exam = relationship("Exam", back_populates="exam_subjects")
    subject = relationship("Subject", back_populates="exam_subjects")
    __table_args__ = (UniqueConstraint("exam_id", "subject_code"),)
