from .auth import router as auth_router
from .exams import router as exams_router
from .results import router as results_router
from .schools import router as schools_router
from .students import router as students_router
from .student_subjects import router as student_subjects_router

__all__ = [
    "auth_router",
    "exams_router",
    "results_router",
    "schools_router",
    "students_router",
    "student_subjects_router",
]