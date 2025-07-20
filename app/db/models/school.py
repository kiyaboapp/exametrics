
from sqlalchemy import Column, String, Integer, ForeignKey, Enum
from sqlalchemy.orm import relationship
from app.db.database import Base
import enum

class SchoolType(str, enum.Enum):
    GOVERNMENT = "GOVERNMENT"
    PRIVATE = "PRIVATE"
    UNKNOWN = "UNKNOWN"

class School(Base):
    __tablename__ = "schools"
    centre_number = Column(String(10), primary_key=True)
    school_name = Column(String(100), nullable=False)
    region_id = Column(Integer, ForeignKey("regions.region_id", onupdate="CASCADE"), nullable=True)
    council_id = Column(Integer, ForeignKey("councils.council_id", onupdate="CASCADE"), nullable=True)
    ward_id = Column(Integer, ForeignKey("wards.ward_id", onupdate="CASCADE"), nullable=True)
    region_name = Column(String(50))
    council_name = Column(String(50))
    ward_name = Column(String(100))
    school_type = Column(Enum(SchoolType), nullable=False)  # MySQL: Native ENUM
    students = relationship("Student", back_populates="school")
    results = relationship("Result", back_populates="school")
    users = relationship("User", back_populates="school")
