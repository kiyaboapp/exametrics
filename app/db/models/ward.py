from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from app.db.database import Base

class Ward(Base):
    __tablename__ = "wards"

    ward_id = Column(Integer, primary_key=True)
    ward_name = Column(String(100), nullable=False)
    council_id = Column(Integer, ForeignKey("councils.council_id", onupdate="CASCADE"), nullable=False)

    # Relationships
    council = relationship("Council", back_populates="wards")
    schools = relationship("School", back_populates="ward")