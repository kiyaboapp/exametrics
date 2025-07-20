
from typing import Optional
from pydantic import BaseModel, ConfigDict

class ExamBoardBase(BaseModel):
    board_id: str
    name: str
    location: Optional[str]=None
    chairman: Optional[str]=None
    secretary: Optional[str]=None

class ExamBoardCreate(ExamBoardBase):
    pass

class ExamBoard(ExamBoardBase):
    model_config = ConfigDict(from_attributes=True)
