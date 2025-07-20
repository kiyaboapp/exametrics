
from sqlalchemy import Column, String, Boolean, ForeignKey, DateTime, text, Enum
from sqlalchemy.orm import relationship
from app.db.database import Base
from uuid6 import uuid6
import enum

class Role(str, enum.Enum):
    USER = "USER"
    TEACHER = "TEACHER"
    ACADEMIC_MASTER = "ACADEMIC_MASTER"
    HEAD_OF_SCHOOL = "HEAD_OF_SCHOOL"
    WEO = "WEO"
    DEO = "DEO"
    REO = "REO"
    ADMIN = "ADMIN"

class User(Base):
    __tablename__ = "users"
    id = Column(String(36), primary_key=True, default=lambda: str(uuid6()))
    username = Column(String(255), unique=True, nullable=False)
    email = Column(String(255), unique=True, nullable=False)
    first_name = Column(String(255), nullable=False)
    middle_name = Column(String(255))
    surname = Column(String(255), nullable=False)
    role = Column(Enum(Role), nullable=False)  # MySQL: Native ENUM
    hashed_password = Column(String(255), nullable=False)
    is_active = Column(Boolean, default=True)
    is_verified = Column(Boolean, default=False)
    centre_number = Column(String(10), ForeignKey("schools.centre_number", ondelete="SET NULL"))
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    school = relationship("School", back_populates="users")
    user_exams = relationship("UserExam", back_populates="user")
