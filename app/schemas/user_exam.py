from pydantic import BaseModel
from enum import Enum
from pydantic import ConfigDict

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
    model_config = ConfigDict(from_attributes=True)

class UserExam(UserExamInDB):
    pass