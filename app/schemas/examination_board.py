from pydantic import BaseModel
from typing import Optional
from uuid import UUID
from pydantic import ConfigDict

class ExamBoardBase(BaseModel):
    name: str
    location: Optional[str] = None
    chairman: Optional[str] = None
    secretary: Optional[str] = None

class ExamBoardCreate(ExamBoardBase):
    pass

class ExamBoard(ExamBoardBase):
    board_id: UUID
    model_config = ConfigDict(from_attributes=True)