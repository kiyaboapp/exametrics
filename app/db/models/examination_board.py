from sqlalchemy import Column, String
from sqlalchemy.orm import relationship
from app.db.database import Base

class ExamBoard(Base):
    __tablename__ = "exam_boards"

    board_id = Column(String(36), primary_key=True, default="uuid()")
    name = Column(String(100), nullable=False)
    location = Column(String(100))
    chairman = Column(String(100))
    secretary = Column(String(100))

    # Relationship
    exams = relationship("Exam", back_populates="board")