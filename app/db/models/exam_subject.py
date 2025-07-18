from sqlalchemy import Column, String, Boolean, ForeignKey,Computed
from sqlalchemy.orm import relationship
from app.db.database import Base

class ExamSubject(Base):
    __tablename__ = "exam_subjects"

    exam_id = Column(String(36), ForeignKey("exams.exam_id", ondelete="CASCADE"), primary_key=True)
    subject_code = Column(String(10), ForeignKey("subjects.subject_code"), primary_key=True)
    subject_name = Column(String(50), nullable=False)
    subject_short = Column(String(50), nullable=False)
    is_present = Column(Boolean, nullable=False, default=False)
    has_practical = Column(Boolean, nullable=False, default=False)
    display_name = Column(
        String(30),
        Computed("CONCAT(subject_code, '-', subject_short)", persisted=True)
    )
    exclude_from_gpa = Column(Boolean, nullable=False, default=False)

    # Relationships
    exam = relationship("Exam", back_populates="subjects")
    subject = relationship("Subject", back_populates="exam_subjects")
    student_subjects = relationship("StudentSubject", back_populates="exam_subject")