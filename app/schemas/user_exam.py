from pydantic import BaseModel
from enum import Enum

class UserExamRole(str, Enum):
    VIEWER = "VIEWER"
    UPLOADER = "UPLOADER"
    EXAM_ADMIN = "EXAM_ADMIN"

class UserExamBase(BaseModel):
    user_id: int
    exam_id: str
    role: UserExamRole

class UserExamCreate(UserExamBase):
    pass

class UserExamInDB(UserExamBase):
    class Config:
        from_attributes = True