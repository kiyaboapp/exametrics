# app/db/models/__init__.py

from .school import School
from .student import Student
from .examination_board import ExamBoard
from .exam import Exam
from .exam_division import ExamDivision
from .exam_grade import ExamGrade
from .subject import Subject
from .exam_subject import ExamSubject
from .result import Result
from .student_subject import StudentSubject
from .user import User
from .user_exam import UserExam
from .region import Region
from .ward import Ward
from .council import Council

__all__ = [
    "School",
    "Student",
    "ExamBoard",
    "Exam",
    "ExamDivision",
    "ExamGrade",
    "Subject",
    "ExamSubject",
    "Result",
    "StudentSubject",
    "User",
    "UserExam",
    "Region",
    "Council",
    "Ward"
]