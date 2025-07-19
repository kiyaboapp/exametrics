from .region import Region
from .council import Council
from .ward import Ward
from .school import School,SchoolType
from .user import User
from .user_exam import UserExam
from .examination_board import ExamBoard
from .exam import Exam
from .exam_division import ExamDivision
from .exam_grade import ExamGrade
from .subject import Subject
from .exam_subject import ExamSubject
from .student import Student
from .result import Result
from .student_subject import StudentSubject

__all__ = [
    "Region",
    "Council",
    "Ward",
    "School",
    "User",
    "UserExam",
    "ExamBoard",
    "Exam",
    "ExamDivision",
    "ExamGrade",
    "Subject",
    "ExamSubject",
    "Student",
    "Result",
    "StudentSubject",
    "SchoolType"
]