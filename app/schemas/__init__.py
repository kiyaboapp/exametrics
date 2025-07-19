from .region import Region, RegionCreate
from .council import Council, CouncilCreate
from .ward import Ward, WardCreate
from .school import School, SchoolCreate
from .examination_board import ExamBoard, ExamBoardCreate
from .exam import Exam, ExamCreate
from .exam_division import ExamDivision, ExamDivisionCreate
from .exam_grade import ExamGrade, ExamGradeCreate
from .exam_subject import ExamSubject, ExamSubjectCreate
from .subject import Subject, SubjectCreate
from .student import Student, StudentCreate
from .result import Result, ResultCreate
from .student_subject import StudentSubject, StudentSubjectCreate
from .user import User, UserCreate
from .user_exam import UserExam, UserExamCreate

__all__ = [
    "Region", "RegionCreate",
    "Council", "CouncilCreate",
    "Ward", "WardCreate",
    "School", "SchoolCreate",
    "ExamBoard", "ExamBoardCreate",
    "Exam", "ExamCreate",
    "ExamDivision", "ExamDivisionCreate",
    "ExamGrade", "ExamGradeCreate",
    "ExamSubject", "ExamSubjectCreate",
    "Subject", "SubjectCreate",
    "Student", "StudentCreate",
    "Result", "ResultCreate",
    "StudentSubject", "StudentSubjectCreate",
    "User", "UserCreate",
    "UserExam", "UserExamCreate",
]