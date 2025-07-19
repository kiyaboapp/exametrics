from sqlalchemy import Column, String, Float, Integer, ForeignKey, DateTime, Index, UniqueConstraint, text
from sqlalchemy.orm import relationship
from app.db.database import Base
from uuid6 import uuid6

class Result(Base):
    __tablename__ = "results"
    id = Column(String(36), primary_key=True, default=lambda: str(uuid6()))
    exam_id = Column(String(36), ForeignKey("exams.exam_id", ondelete="CASCADE"))
    student_global_id = Column(String(36), ForeignKey("students.student_global_id", ondelete="CASCADE"))
    centre_number = Column(String(10), ForeignKey("schools.centre_number", ondelete="CASCADE"))
    avg_marks = Column(Float)
    total_marks = Column(Float)
    division = Column(String(3))
    total_points = Column(Integer)
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
    ward_pos_unknown = Column(Integer)
    council_pos_gvt = Column(Integer)
    council_pos_pvt = Column(Integer)
    council_pos_unknown = Column(Integer)
    region_pos_gvt = Column(Integer)
    region_pos_pvt = Column(Integer)
    region_pos_unknown = Column(Integer)
    first_marks = Column(Float)
    second_marks = Column(Float)
    third_marks = Column(Float)
    fourth_marks = Column(Float)
    fifth_marks = Column(Float)
    sixth_marks = Column(Float)
    seventh_marks = Column(Float)
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    
    # Relationships
    student = relationship("Student", back_populates="results")
    exam = relationship("Exam", back_populates="results")
    
    __table_args__ = (
        UniqueConstraint('exam_id', 'student_global_id', 'centre_number'),
        Index('idx_result_exam_id', 'exam_id'),
        Index('idx_result_student_global_id', 'student_global_id'),
        Index('idx_result_centre_number', 'centre_number'),
    )