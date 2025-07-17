from pydantic import BaseModel
from typing import Optional

class ExamBoardBase(BaseModel):
    name: str
    location: Optional[str] = None
    chairman: Optional[str] = None
    secretary: Optional[str] = None

class ExamBoardCreate(ExamBoardBase):
    pass

class ExamBoardInDB(ExamBoardBase):
    board_id: str

    class Config:
        from_attributes = True