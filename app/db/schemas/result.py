
from typing import Optional
from pydantic import BaseModel, ConfigDict
from datetime import datetime

class ResultBase(BaseModel):
    id: str
    exam_id: str
    student_global_id: str
    centre_number: str
    avg_marks: float | None
    total_marks: float | None
    division: Optional[str]=None
    total_points: Optional[int]=None
    pos: Optional[int]=None
    out_of: Optional[int]=None
    ward_pos: Optional[int]=None
    ward_out_of: Optional[int]=None
    council_pos: Optional[int]=None
    council_out_of: Optional[int]=None
    region_pos: Optional[int]=None
    region_out_of: Optional[int]=None
    ward_pos_gvt: Optional[int]=None
    ward_pos_pvt: Optional[int]=None
    council_pos_gvt: Optional[int]=None
    council_pos_pvt: Optional[int]=None
    region_pos_gvt: Optional[int]=None
    region_pos_pvt: Optional[int]=None
    created_at: datetime

class ResultCreate(BaseModel):
    exam_id: str
    student_global_id: str
    centre_number: str
    avg_marks: float | None
    total_marks: float | None
    division: Optional[str]=None
    total_points: Optional[int]=None
    pos: Optional[int]=None
    out_of: Optional[int]=None
    ward_pos: Optional[int]=None
    ward_out_of: Optional[int]=None
    council_pos: Optional[int]=None
    council_out_of: Optional[int]=None
    region_pos: Optional[int]=None
    region_out_of: Optional[int]=None
    ward_pos_gvt: Optional[int]=None
    ward_pos_pvt: Optional[int]=None
    council_pos_gvt: Optional[int]=None
    council_pos_pvt: Optional[int]=None
    region_pos_gvt: Optional[int]=None
    region_pos_pvt: Optional[int]=None

class Result(ResultBase):
    model_config = ConfigDict(from_attributes=True)
