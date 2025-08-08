
from sqlalchemy import Column, String, Float, ForeignKey, Integer, Index, UniqueConstraint,DateTime,text
from sqlalchemy.orm import relationship
from app.db.database import Base
from uuid6 import uuid6

class StudentSubject(Base):
    __tablename__ = "student_subjects"
    id = Column(String(36), primary_key=True, default=lambda: str(uuid6()))
    exam_id = Column(String(36), ForeignKey("exams.exam_id", ondelete="CASCADE"))
    student_global_id = Column(String(36), ForeignKey("students.student_global_id", ondelete="CASCADE"))
    centre_number = Column(String(10), ForeignKey("schools.centre_number", ondelete="CASCADE"))
    subject_code = Column(String(10), ForeignKey("exam_subjects.subject_code", ondelete="CASCADE"))
    theory_marks = Column(Float)
    practical_marks = Column(Float)
    overall_marks = Column(Float)
    subject_grade=Column(String(10))
    subject_pos = Column(Integer)  # School-level position for this subject
    subject_out_of = Column(Integer)  # Total students for this subject at school
    ward_subject_pos = Column(Integer)  # Ward-level position for this subject
    ward_subject_out_of = Column(Integer)  # Total students for this subject in ward
    council_subject_pos = Column(Integer)  # Council-level position for this subject
    council_subject_out_of = Column(Integer)  # Total students for this subject in council
    region_subject_pos = Column(Integer)  # Region-level position for this subject
    region_subject_out_of = Column(Integer)  # Total students for this subject in region
    ward_subject_pos_gvt = Column(Integer)  # Ward-level position for government schools
    ward_subject_out_of_gvt=Column(Integer)
    ward_subject_pos_pvt = Column(Integer)  # Ward-level position for private schools
    ward_subject_out_of_pvt=Column(Integer)
    council_subject_pos_gvt = Column(Integer)  # Council-level position for government schools
    council_subject_out_of_gvt=Column(Integer)
    council_subject_pos_pvt = Column(Integer)  # Council-level position for private schools
    council_subject_out_of_pvt=Column(Integer)
    region_subject_pos_gvt = Column(Integer)  # Region-level position for government schools
    region_subject_out_of_gvt=Column(Integer)
    region_subject_pos_pvt = Column(Integer)  # Region-level position for private schools
    region_subject_out_of_pvt=Column(Integer)
    school_pos=Column(Integer)
    school_out_of=Column(Integer)  # Total students for this subject at school

    
    # RELATIONSHIPS
    submitted_by=Column(String(50))
    submitted_on = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    student = relationship("Student", back_populates="student_subjects")
    exam = relationship("Exam")
    school = relationship("School")
    exam_subject = relationship("ExamSubject")
    __table_args__ = (
        UniqueConstraint("exam_id", "student_global_id", "centre_number", "subject_code"),
        Index("idx_student_subject_exam_id", "exam_id"),
        Index("idx_student_subject_student_global_id", "student_global_id"),
        Index("idx_student_subject_centre_number", "centre_number"),
        Index("idx_student_subject_subject_code", "subject_code"),
    )
