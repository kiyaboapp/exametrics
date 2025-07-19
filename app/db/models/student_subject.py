from sqlalchemy import Column, String, Float, Integer, ForeignKey, DateTime, Index, UniqueConstraint, text
from sqlalchemy.orm import relationship
from app.db.database import Base
from uuid6 import uuid6

class StudentSubject(Base):
    __tablename__ = "student_subjects"
    id = Column(String(36), primary_key=True, default=lambda: str(uuid6()))
    exam_id = Column(String(36), ForeignKey("exams.exam_id", ondelete="CASCADE"))
    student_global_id = Column(String(36), ForeignKey("students.student_global_id", ondelete="CASCADE"))
    centre_number = Column(String(10), ForeignKey("schools.centre_number", ondelete="CASCADE"))
    subject_code = Column(String(10), ForeignKey("subjects.subject_code", ondelete="CASCADE"))
    theory_marks = Column(Float)
    practical_marks = Column(Float)
    overall_marks = Column(Float)
    subj_pos = Column(Integer)
    subj_ward_pos = Column(Integer)
    subj_council_pos = Column(Integer)
    subj_region_pos = Column(Integer)
    subj_ward_pos_gvt = Column(Integer)
    subj_ward_pos_pvt = Column(Integer)
    subj_ward_pos_unknown = Column(Integer)
    subj_council_pos_gvt = Column(Integer)
    subj_council_pos_pvt = Column(Integer)
    subj_council_pos_unknown = Column(Integer)
    subj_region_pos_gvt = Column(Integer)
    subj_region_pos_pvt = Column(Integer)
    subj_region_pos_unknown = Column(Integer)
    submitted_by = Column(String(100))
    subj_pos_out_of = Column(Integer)
    subj_ward_pos_out_of = Column(Integer)
    subj_council_pos_out_of = Column(Integer)
    subj_region_pos_out_of = Column(Integer)
    subj_ward_pos_gvt_out_of = Column(Integer)
    subj_ward_pos_pvt_out_of = Column(Integer)
    subj_ward_pos_unknown_out_of = Column(Integer)
    subj_council_pos_gvt_out_of = Column(Integer)
    subj_council_pos_pvt_out_of = Column(Integer)
    subj_council_pos_unknown_out_of = Column(Integer)
    subj_region_pos_gvt_out_of = Column(Integer)
    subj_region_pos_pvt_out_of = Column(Integer)
    subj_region_pos_unknown_out_of = Column(Integer)
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    
    # Relationships
    student = relationship("Student", back_populates="student_subjects")
    exam = relationship("Exam", back_populates="student_subjects")
    exam_subject = relationship("ExamSubject", back_populates="student_subjects")
    
    __table_args__ = (
        UniqueConstraint('exam_id', 'centre_number', 'student_global_id', 'subject_code'),
        Index('idx_student_subject_exam_id', 'exam_id'),
        Index('idx_student_subject_student_global_id', 'student_global_id'),
        Index('idx_student_subject_subject_code', 'subject_code'),
    )