from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, Index, UniqueConstraint, text
from sqlalchemy.orm import relationship
from app.db.database import Base

class ExamSubject(Base):
    __tablename__ = "exam_subjects"
    id = Column(Integer, primary_key=True, autoincrement=True)
    exam_id = Column(String(36), ForeignKey("exams.exam_id", ondelete="CASCADE"))
    subject_code = Column(String(10), ForeignKey("subjects.subject_code", onupdate="CASCADE"))
    subject_name = Column(String(50), nullable=False)
    subject_short = Column(String(50), nullable=False)
    is_present = Column(Boolean, nullable=True)
    has_practical = Column(Boolean, nullable=False)
    display_name = Column(String(30), server_default=text("CONCAT(subject_code, '-', subject_short)"))
    exclude_from_gpa = Column(Boolean, nullable=False)
    
    # Relationships
    exam = relationship("Exam", back_populates="exam_subjects")
    subject = relationship("Subject", back_populates="exam_subjects")
    student_subjects = relationship("StudentSubject", back_populates="exam_subject")
    
    __table_args__ = (
        UniqueConstraint('exam_id', 'subject_code'),
        Index('idx_exam_subject_exam_id', 'exam_id'),
        Index('idx_exam_subject_subject_code', 'subject_code'),
    )