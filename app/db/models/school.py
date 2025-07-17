from sqlalchemy import Column, String, Integer, Enum, ForeignKey
from sqlalchemy.orm import relationship
from app.db.database import Base
import enum

class SchoolType(str, enum.Enum):
    GOVERNMENT = "government"
    PRIVATE = "private"
    UNKNOWN = "unknown"

class School(Base):
    __tablename__ = "schools"

    centre_number = Column(String(10), primary_key=True)
    school_name = Column(String(100), nullable=False)
    region_id = Column(Integer, ForeignKey("regions.region_id", ondelete="SET NULL", onupdate="CASCADE"))
    council_id = Column(Integer, ForeignKey("councils.council_id", ondelete="SET NULL", onupdate="CASCADE"))
    ward_id = Column(Integer, ForeignKey("wards.ward_id", ondelete="SET NULL", onupdate="CASCADE"))
    school_type = Column(Enum(SchoolType), nullable=False, default=SchoolType.UNKNOWN)

    # Relationships
    region = relationship("Region", back_populates="schools")
    council = relationship("Council", back_populates="schools")
    ward = relationship("Ward", back_populates="schools")
    students = relationship("Student", back_populates="school")
    users = relationship("User", back_populates="school")