from pydantic import BaseModel, EmailStr
from pydantic import ConfigDict
from typing import Optional
from datetime import datetime
from enum import Enum

from uuid import UUID

class UserRole(str, Enum):
    USER = "USER"
    TEACHER = "TEACHER"
    ACADEMIC_MASTER = "ACADEMIC_MASTER"
    HEAD_OF_SCHOOL = "HEAD_OF_SCHOOL"
    WEO = "WEO"
    DEO = "DEO"
    REO = "REO"
    ADMIN = "ADMIN"

class UserBase(BaseModel):
    username: str
    email: EmailStr
    first_name: str
    middle_name: Optional[str] = None
    surname: str
    # school_name: Optional[str] = None
    centre_number: Optional[str] = None
    role: UserRole = UserRole.USER

class UserCreate(UserBase):
    password: str


class UserUpdate(BaseModel):
    first_name: Optional[str] = None
    middle_name: Optional[str] = None
    surname: Optional[str] = None
    # school_name: Optional[str] = None
    centre_number: Optional[str] = None
    role: Optional[UserRole] = None
    is_active: Optional[bool] = None
    is_verified: Optional[bool] = None

class UserInDB(UserBase):
    id: UUID
    hashed_password: str
    created_at: datetime
    updated_at: datetime
    is_active: bool
    is_verified: bool
    last_login: Optional[datetime] = None
    model_config = ConfigDict(from_attributes=True)

class User(UserInDB):
    pass
