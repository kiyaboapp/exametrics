from pydantic import BaseModel
from uuid import UUID
from typing import Optional
from datetime import datetime
from pydantic import ConfigDict

class StudentSubjectBase(BaseModel):
    exam_id: UUID
    student_global_id: UUID
    centre_number: str
    subject_code: str
    theory_marks: Optional[float] = None
    practical_marks: Optional[float] = None
    overall_marks: Optional[float] = None
    subj_pos: Optional[int] = None
    subj_ward_pos: Optional[int] = None
    subj_council_pos: Optional[int] = None
    subj_region_pos: Optional[int] = None
    subj_ward_pos_gvt: Optional[int] = None
    subj_ward_pos_pvt: Optional[int] = None
    subj_ward_pos_unknown: Optional[int] = None
    subj_council_pos_gvt: Optional[int] = None
    subj_council_pos_pvt: Optional[int] = None
    subj_council_pos_unknown: Optional[int] = None
    subj_region_pos_gvt: Optional[int] = None
    subj_region_pos_pvt: Optional[int] = None
    subj_region_pos_unknown: Optional[int] = None
    submitted_by: Optional[str] = None
    subj_pos_out_of: Optional[int] = None
    subj_ward_pos_out_of: Optional[int] = None
    subj_council_pos_out_of: Optional[int] = None
    subj_region_pos_out_of: Optional[int] = None
    subj_ward_pos_gvt_out_of: Optional[int] = None
    subj_ward_pos_pvt_out_of: Optional[int] = None
    subj_ward_pos_unknown_out_of: Optional[int] = None
    subj_council_pos_gvt_out_of: Optional[int] = None
    subj_council_pos_pvt_out_of: Optional[int] = None
    subj_council_pos_unknown_out_of: Optional[int] = None
    subj_region_pos_gvt_out_of: Optional[int] = None
    subj_region_pos_pvt_out_of: Optional[int] = None
    subj_region_pos_unknown_out_of: Optional[int] = None

class StudentSubjectCreate(StudentSubjectBase):
    pass

class StudentSubject(StudentSubjectBase):
    id: UUID
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)