from sqlalchemy import Column, String, Float, Integer, ForeignKey
from sqlalchemy.orm import relationship
from app.db.database import Base

class Result(Base):
    __tablename__ = "results"

    exam_id = Column(String(36), ForeignKey("exams.exam_id"), primary_key=True)
    student_id = Column(String(20), ForeignKey("students.student_id"), primary_key=True)  # Added ForeignKey
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

    # Relationships
    exam = relationship("Exam", back_populates="results")
    student = relationship("Student", back_populates="results")