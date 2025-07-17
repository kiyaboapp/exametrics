from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from app.db.database import Base

class Council(Base):
    __tablename__ = "councils"

    council_id = Column(Integer, primary_key=True, autoincrement=True)
    council_name = Column(String(50), unique=True, nullable=False)
    region_id = Column(Integer, ForeignKey("regions.region_id", onupdate="CASCADE"), nullable=False)

    # Relationships
    region = relationship("Region", back_populates="councils")
    wards = relationship("Ward", back_populates="council")
    schools = relationship("School", back_populates="council")