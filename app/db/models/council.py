
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from app.db.database import Base

class Council(Base):
    __tablename__ = "councils"
    council_id = Column(Integer, primary_key=True, autoincrement=True)
    council_name = Column(String(50), nullable=False)
    region_id = Column(Integer, ForeignKey("regions.region_id", onupdate="CASCADE"), nullable=False)
    region = relationship("Region", back_populates="councils")
    wards = relationship("Ward", back_populates="council")
