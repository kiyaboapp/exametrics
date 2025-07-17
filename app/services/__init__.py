from .auth_service import create_user, authenticate_user
from .exam_service import create_exam, get_exam
from .result_service import create_result, get_result
from .school_service import create_school, get_school
from .student_service import create_student, get_student

__all__ = [
    "create_user", "authenticate_user",
    "create_exam", "get_exam",
    "create_result", "get_result",
    "create_school", "get_school",
    "create_student", "get_student",
]