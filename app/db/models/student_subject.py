from sqlalchemy import Column, ForeignKeyConstraint, String, Float, Integer, ForeignKey
from sqlalchemy.orm import relationship
from app.db.database import Base

class StudentSubject(Base):
    __tablename__ = "student_subjects"

    exam_id = Column(String(36), ForeignKey("exams.exam_id"), primary_key=True)
    student_id = Column(String(20), ForeignKey("students.student_id"), primary_key=True)  # Added ForeignKey
    subject_code = Column(String(10), primary_key=True)
    __table_args__ = (
        ForeignKeyConstraint(
            ["exam_id", "subject_code"],
            ["exam_subjects.exam_id", "exam_subjects.subject_code"],
            ondelete="CASCADE"
        ),
    )

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

    # Relationships
    exam = relationship("Exam", back_populates="student_subjects")
    student = relationship("Student", back_populates="student_subjects")
    exam_subject = relationship("ExamSubject", back_populates="student_subjects")