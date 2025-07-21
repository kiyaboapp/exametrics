
from pydantic import BaseModel, ConfigDict
from typing import Dict

class UserExamBase(BaseModel):
    user_id: str
    exam_id: str
    role: str
    permissions: Dict[str, bool]

class UserExamCreate(BaseModel):
    user_id: str
    exam_id: str
    role: str
    permissions: Dict[str, bool] = {
        "edit": False,
        "view": True,
        "upload_results": False,
        "download_analysis": False,
        "view_progress": False,
        "manage_users": False
    }

class UserExam(UserExamBase):
    model_config = ConfigDict(from_attributes=True)
