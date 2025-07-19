from sqlalchemy import Column, Integer, String, Enum, DateTime, ForeignKey, Index, UniqueConstraint, text
from sqlalchemy.orm import relationship
from app.db.database import Base
import enum
from uuid6 import uuid6

class Sex(str, enum.Enum):
    MALE = "M"
    FEMALE = "F"
    OTHER = "OTHER"

class Student(Base):
    __tablename__ = "students"
    student_global_id=Column(String(36),primary_key=True,default=lambda: str(uuid6()))
    exam_id = Column(String(36), ForeignKey("exams.exam_id", ondelete="CASCADE"), nullable=False)
    student_id = Column(String(20), nullable=False)
    centre_number = Column(String(10), ForeignKey("schools.centre_number", ondelete="CASCADE"), nullable=False)
    first_name = Column(String(50), nullable=False)
    middle_name = Column(String(50))
    surname = Column(String(50), nullable=False)
    full_name = Column(String(150), server_default=text("CONCAT(first_name, ' ', COALESCE(middle_name, ''), ' ', surname)"))
    sex = Column(Enum(Sex), nullable=False)
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))

    # RELATIONSHIP
    school = relationship("School", back_populates="students")
    exam = relationship("Exam", back_populates="students")
    results = relationship("Result", back_populates="student")
    student_subjects = relationship("StudentSubject", back_populates="student")

    # UNIQUE TOGETEHR
    __table_args__ = (
        UniqueConstraint('exam_id', 'student_id', 'centre_number'),
    )