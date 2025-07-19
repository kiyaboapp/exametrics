from sqlalchemy import Column, String, Enum, ForeignKey, JSON
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
    role = Column(Enum(UserExamRole), nullable=False)
    permissions = Column(JSON, nullable=False, default=lambda: {
        "edit": False,
        "view": True,
        "upload_results": False,
        "download_analysis": False,
        "view_progress": False,
        "manage_users": False,
    })
    
    # Relationships
    user = relationship("User", back_populates="user_exams")
    exam = relationship("Exam", back_populates="user_exams")
    
    def has_permission(self, perm_name: str) -> bool:
        if self.role == UserExamRole.EXAM_ADMIN:
            return True
        return self.permissions.get(perm_name, False)
    
    def set_permission(self, perm_name: str, value: bool) -> None:
        self.permissions[perm_name] = value
    
    def all_permissions(self):
        if self.role == UserExamRole.EXAM_ADMIN:
            return {key: True for key in self.permissions.keys()}
        return self.permissions