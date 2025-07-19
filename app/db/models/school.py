from sqlalchemy import Column, String, Enum, Index
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
    region_name = Column(String(50), index=True)
    council_name = Column(String(50), index=True)
    ward_name = Column(String(100), index=True)

    # RELATIONSHIPS
    school_type = Column(Enum(SchoolType), nullable=False)
    students = relationship("Student", back_populates="school")
    users = relationship("User", back_populates="school")
    