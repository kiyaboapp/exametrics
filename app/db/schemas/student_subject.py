
from typing import Optional
from pydantic import BaseModel, ConfigDict

class StudentSubjectBase(BaseModel):
    id: str
    exam_id: str
    student_global_id: str
    centre_number: str
    subject_code: str
    theory_marks: float | None
    practical_marks: float | None
    overall_marks: float | None
    subject_pos: Optional[int]=None
    subject_out_of: Optional[int]=None
    ward_subject_pos: Optional[int]=None
    ward_subject_out_of: Optional[int]=None
    council_subject_pos: Optional[int]=None
    council_subject_out_of: Optional[int]=None
    region_subject_pos: Optional[int]=None
    region_subject_out_of: Optional[int]=None
    ward_subject_pos_gvt: Optional[int]=None
    ward_subject_pos_pvt: Optional[int]=None
    council_subject_pos_gvt: Optional[int]=None
    council_subject_pos_pvt: Optional[int]=None
    region_subject_pos_gvt: Optional[int]=None
    region_subject_pos_pvt: Optional[int]=None

class StudentSubjectCreate(BaseModel):
    exam_id: str
    student_global_id: str
    centre_number: str
    subject_code: str
    theory_marks: float | None
    practical_marks: float | None
    overall_marks: float | None
    subject_pos: Optional[int]=None
    subject_out_of: Optional[int]=None
    ward_subject_pos: Optional[int]=None
    ward_subject_out_of: Optional[int]=None
    council_subject_pos: Optional[int]=None
    council_subject_out_of: Optional[int]=None
    region_subject_pos: Optional[int]=None
    region_subject_out_of: Optional[int]=None
    ward_subject_pos_gvt: Optional[int]=None
    ward_subject_pos_pvt: Optional[int]=None
    council_subject_pos_gvt: Optional[int]=None
    council_subject_pos_pvt: Optional[int]=None
    region_subject_pos_gvt: Optional[int]=None
    region_subject_pos_pvt: Optional[int]=None

class StudentSubject(StudentSubjectBase):
    model_config = ConfigDict(from_attributes=True)
