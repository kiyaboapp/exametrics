# app/db/models/__init__.py

from .school import School, SchoolType
from .student import Student, Sex
from .examination_board import ExamBoard
from .exam import Exam, AvgStyle, ExamLevel
from .exam_division import ExamDivision
from .exam_grade import ExamGrade
from .subject import Subject
from .exam_subject import ExamSubject
from .result import Result
from .student_subject import StudentSubject
from .user import User, Role
from .user_exam import UserExam, UserExamRole
from .region import Region
from .ward import Ward
from .council import Council

__all__ = [
    "School",
    "SchoolType",
    "Student",
    "Sex",
    "ExamBoard",
    "Exam",
    "AvgStyle",
    "ExamLevel",
    "ExamDivision",
    "ExamGrade",
    "Subject",
    "ExamSubject",
    "Result",
    "StudentSubject",
    "User",
    "Role",
    "UserExam",
    "UserExamRole",
    "Region",
    "Council",
    "Ward"
]