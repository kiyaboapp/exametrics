from sqlalchemy import Column, Integer, String, Enum, DateTime, Boolean, ForeignKey
from sqlalchemy.sql import text
from sqlalchemy.orm import relationship
from app.db.base import Base
import enum
from uuid6 import uuid6

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
    role = Column(Enum(Role), nullable=False)
    hashed_password = Column(String(255), nullable=False)
    created_at = Column(DateTime, nullable=False, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(DateTime, nullable=False, server_default=text('CURRENT_TIMESTAMP'))
    is_active = Column(Boolean, nullable=False, default=True)
    is_verified = Column(Boolean, nullable=False, default=False)
    verification_token = Column(String(255))
    verification_token_expires = Column(DateTime)
    reset_token = Column(String(255))
    reset_token_expires = Column(DateTime)
    google_id = Column(String(255), unique=True)
    is_google_account = Column(Boolean, default=False)
    last_login = Column(DateTime)

    # Foreign key to School
    centre_number = Column(String(10), ForeignKey("schools.centre_number", ondelete="SET NULL"))

    # Relationships
    school = relationship("School", back_populates="users")
    user_exams = relationship("UserExam", back_populates="user")