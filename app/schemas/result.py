from pydantic import BaseModel
from uuid import UUID
from typing import Optional
from datetime import datetime
from pydantic import ConfigDict

class ResultBase(BaseModel):
    exam_id: UUID
    student_global_id: UUID
    centre_number: str
    avg_marks: Optional[float] = None
    total_marks: Optional[float] = None
    division: Optional[str] = None
    total_points: Optional[int] = None
    pos: Optional[int] = None
    out_of: Optional[int] = None
    ward_pos: Optional[int] = None
    ward_out_of: Optional[int] = None
    council_pos: Optional[int] = None
    council_out_of: Optional[int] = None
    region_pos: Optional[int] = None
    region_out_of: Optional[int] = None
    ward_pos_gvt: Optional[int] = None
    ward_pos_pvt: Optional[int] = None
    ward_pos_unknown: Optional[int] = None
    council_pos_gvt: Optional[int] = None
    council_pos_pvt: Optional[int] = None
    council_pos_unknown: Optional[int] = None
    region_pos_gvt: Optional[int] = None
    region_pos_pvt: Optional[int] = None
    region_pos_unknown: Optional[int] = None
    first_marks: Optional[float] = None
    second_marks: Optional[float] = None
    third_marks: Optional[float] = None
    fourth_marks: Optional[float] = None
    fifth_marks: Optional[float] = None
    sixth_marks: Optional[float] = None
    seventh_marks: Optional[float] = None

class ResultCreate(ResultBase):
    pass

class Result(ResultBase):
    id: UUID
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)