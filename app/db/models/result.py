
from sqlalchemy import Column, String, Integer, Float, ForeignKey, DateTime, Index, UniqueConstraint, text
from sqlalchemy.orm import relationship
from app.db.database import Base
from uuid6 import uuid6

class Result(Base):
    __tablename__ = "results"
    id = Column(String(36), primary_key=True, default=lambda: str(uuid6()))
    exam_id = Column(String(36), ForeignKey("exams.exam_id", ondelete="CASCADE"))
    student_global_id = Column(String(36), ForeignKey("students.student_global_id", ondelete="CASCADE"))
    centre_number = Column(String(10), ForeignKey("schools.centre_number", ondelete="CASCADE"))

    # TOTALS AND SUMMATIONS
    avg_marks = Column(Float)
    avg_grade=Column(String(10))
    total_marks = Column(Float)
    division = Column(String(3))
    total_points = Column(Integer)

    # RANKINGS
    pos = Column(Integer)
    out_of = Column(Integer)
    ward_pos = Column(Integer)
    ward_out_of = Column(Integer)
    council_pos = Column(Integer)
    council_out_of = Column(Integer)
    region_pos = Column(Integer)
    region_out_of = Column(Integer)
    ward_pos_gvt = Column(Integer)
    ward_pos_pvt = Column(Integer)
    council_pos_gvt = Column(Integer)
    council_pos_pvt = Column(Integer)
    region_pos_gvt = Column(Integer)
    region_pos_pvt = Column(Integer)
    school_pos=Column(Integer)
    school_out_of=Column(Integer)

    # READ-ONLY FIELDS
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    student = relationship("Student", back_populates="results")
    exam = relationship("Exam", back_populates="results")
    school = relationship("School", back_populates="results")
    __table_args__ = (
        UniqueConstraint("exam_id", "student_global_id", "centre_number"),
        Index("idx_result_exam_id", "exam_id"),
        Index("idx_result_student_global_id", "student_global_id"),
        Index("idx_result_centre_number", "centre_number"),
    )
