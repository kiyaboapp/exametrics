
from sqlalchemy import Column, String, ForeignKey, JSON,CheckConstraint
from sqlalchemy.orm import relationship
from app.db.database import Base

class UserExam(Base):
    __tablename__ = "user_exams"
    user_id = Column(String(36), ForeignKey("users.id", ondelete="CASCADE"), primary_key=True)
    exam_id = Column(String(36), ForeignKey("exams.exam_id", ondelete="CASCADE"), primary_key=True)
    role = Column(String(20), nullable=False, default="VIEWER") 
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

    __table_args__=(
        CheckConstraint(
            "role IN ('VIEWER', 'UPLOADER', 'EXAM_ADMIN')", 
            name="valid_role"
    ),
    )
