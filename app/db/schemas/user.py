
from typing import Optional
from pydantic import BaseModel, ConfigDict
from datetime import datetime


class UserBase(BaseModel):
    id: str
    username: str
    email: str
    first_name: str
    middle_name: Optional[str]= None
    surname: str
    role: str
    hashed_password: str
    is_active: bool
    is_verified: bool
    centre_number: Optional[str] = None
    created_at: datetime

class UserCreate(BaseModel):
    username: str
    email: str
    first_name: str
    middle_name: Optional[str]= None
    surname: str
    role: str
    password: str
    is_active: bool = True
    is_verified: bool = False
    centre_number: Optional[str] = None

class User(UserBase):
    model_config = ConfigDict(from_attributes=True)
