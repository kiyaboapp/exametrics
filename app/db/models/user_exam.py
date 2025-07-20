
from sqlalchemy import Column, String, ForeignKey, JSON, Enum
from sqlalchemy.orm import relationship
from app.db.database import Base
import enum

class UserExamRole(str, enum.Enum):
    VIEWER = "VIEWER"
    UPLOADER = "UPLOADER"
    EXAM_ADMIN = "EXAM_ADMIN"

class UserExam(Base):
    __tablename__ = "user_exams"
    user_id = Column(String(36), ForeignKey("users.id", ondelete="CASCADE"), primary_key=True)
    exam_id = Column(String(36), ForeignKey("exams.exam_id", ondelete="CASCADE"), primary_key=True)
    role = Column(Enum(UserExamRole), nullable=False)  # MySQL: Native ENUM
    permissions = Column(JSON, nullable=False, default=lambda: {
        "edit": False,
        "view": True,
        "upload_results": False,
        "download_analysis": False,
        "view_progress": False,
        "manage_users": False
    })
    user = relationship("User", back_populates="user_exams")
    exam = relationship("Exam", back_populates="user_exams")
