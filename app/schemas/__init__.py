from .region import RegionCreate, RegionInDB
from .council import CouncilCreate, CouncilInDB
from .ward import WardCreate, WardInDB
from .school import SchoolCreate, SchoolInDB
from .user import UserCreate, UserInDB, UserUpdate
from .user_exam import UserExamCreate, UserExamInDB
from .examination_board import ExamBoardCreate, ExamBoardInDB
from .exam import ExamCreate, ExamInDB
from .exam_division import ExamDivisionCreate, ExamDivisionInDB
from .exam_grade import ExamGradeCreate, ExamGradeInDB
from .subject import SubjectCreate, SubjectInDB
from .exam_subject import ExamSubjectCreate, ExamSubjectInDB
from .student import StudentCreate, StudentInDB
from .result import ResultCreate, ResultInDB
from .student_subject import StudentSubjectCreate, StudentSubjectInDB

__all__ = [
    "RegionCreate", "RegionInDB",
    "CouncilCreate", "CouncilInDB",
    "WardCreate", "WardInDB",
    "SchoolCreate", "SchoolInDB",
    "UserCreate", "UserInDB", "UserUpdate",
    "UserExamCreate", "UserExamInDB",
    "ExamBoardCreate", "ExamBoardInDB",
    "ExamCreate", "ExamInDB",
    "ExamDivisionCreate", "ExamDivisionInDB",
    "ExamGradeCreate", "ExamGradeInDB",
    "SubjectCreate", "SubjectInDB",
    "ExamSubjectCreate", "ExamSubjectInDB",
    "StudentCreate", "StudentInDB",
    "ResultCreate", "ResultInDB",
    "StudentSubjectCreate", "StudentSubjectInDB",
]