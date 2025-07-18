from sqlalchemy import Column, String, Enum, Date, DateTime, ForeignKey, Computed, text
from sqlalchemy.orm import relationship
from app.db.database import Base
import enum

class Sex(str, enum.Enum):
    MALE = "M"
    FEMALE = "F"
    OTHER = "Other"

class Student(Base):
    __tablename__ = "students"

    student_global_id = Column(String(36), primary_key=True, default="uuid()")
    student_id = Column(String(20), nullable=False, index=True)
    first_name = Column(String(50), nullable=False)
    middle_name = Column(String(50))
    surname = Column(String(50), nullable=False)
    full_name = Column(
        String(150),
        Computed("CONCAT(first_name, ' ', COALESCE(middle_name, ''), ' ', surname)", persisted=True)
    )
    sex = Column(Enum(Sex), nullable=False)
    centre_number = Column(String(10), ForeignKey("schools.centre_number"), nullable=False)
    date_of_birth = Column(Date)
    created_at = Column(DateTime, nullable=False, server_default=text('CURRENT_TIMESTAMP'))

    # Relationships
    school = relationship("School", back_populates="students")
    results = relationship("Result", back_populates="student")
    student_subjects = relationship("StudentSubject", back_populates="student")