from sqlalchemy import Column, Integer, String, Enum, ForeignKey
from sqlalchemy.orm import relationship
from app.db.database import Base
import enum

class UserExamRole(str, enum.Enum):
    VIEWER = "VIEWER"
    UPLOADER = "UPLOADER"
    EXAM_ADMIN = "EXAM_ADMIN"

class UserExam(Base):
    __tablename__ = "user_exams"

    user_id = Column(Integer, ForeignKey("users.id"), primary_key=True)
    exam_id = Column(String(36), ForeignKey("exams.exam_id"), primary_key=True)
    role = Column(Enum(UserExamRole), nullable=False)

    # Relationships
    user = relationship("User", back_populates="user_exams")
    exam = relationship("Exam", back_populates="user_exams")