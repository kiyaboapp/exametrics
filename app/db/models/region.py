from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship
from app.db.database import Base

class Region(Base):
    __tablename__ = "regions"

    region_id = Column(Integer, primary_key=True, autoincrement=True)
    region_name = Column(String(50), unique=True, nullable=False)

    # Relationships
    councils = relationship("Council", back_populates="region")
    schools = relationship("School", back_populates="region")