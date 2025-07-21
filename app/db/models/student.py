
from sqlalchemy import Column, Computed, String, ForeignKey, DateTime, UniqueConstraint, text,CheckConstraint
from sqlalchemy.orm import relationship
from app.db.database import Base
from uuid6 import uuid6


class Student(Base):
    __tablename__ = "students"
    student_global_id = Column(String(36), primary_key=True, default=lambda: str(uuid6()))
    exam_id = Column(String(36), ForeignKey("exams.exam_id", ondelete="CASCADE"), nullable=False)
    student_id = Column(String(20), nullable=False)
    centre_number = Column(String(10), ForeignKey("schools.centre_number", ondelete="CASCADE"), nullable=False)
    first_name = Column(String(50), nullable=False)
    middle_name = Column(String(50))
    surname = Column(String(50), nullable=False)
    sex = Column(String(5), nullable=False) 
    full_name = Column(
        String(160),
        Computed("CONCAT(COALESCE(first_name, ''), ' ', COALESCE(middle_name, ''), ' ', COALESCE(surname, ''))", 
                persisted=True)
    )
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    school = relationship("School", back_populates="students")
    exam = relationship("Exam", back_populates="students")
    student_subjects = relationship("StudentSubject", back_populates="student")
    results = relationship("Result", back_populates="student")

    __table_args__ = (
        UniqueConstraint("exam_id", "student_id", "centre_number"),
        CheckConstraint(
            "sex IN ('M', 'F', 'OTHER')", 
            name="valid_sex"
        ),
    )
