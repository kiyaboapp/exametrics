
from .region import router as region_router
from .council import router as council_router
from .ward import router as ward_router
from .school import router as school_router
from .examination_board import router as examination_board_router
from .exam import router as exam_router
from .exam_division import router as exam_division_router
from .exam_grade import router as exam_grade_router
from .exam_subject import router as exam_subject_router
from .subject import router as subject_router
from .student import router as student_router
from .result import router as result_router
from .student_subject import router as student_subject_router
from .user import router as user_router
from .user_exam import router as user_exam_router
from .auth import router as auth_router

__all__ = [
    "region_router",
    "council_router",
    "ward_router",
    "school_router",
    "examination_board_router",
    "exam_router",
    "exam_division_router",
    "exam_grade_router",
    "exam_subject_router",
    "subject_router",
    "student_router",
    "result_router",
    "student_subject_router",
    "user_router",
    "user_exam_router",
    "auth_router",
]
