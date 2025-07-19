# app/api/v1/__init__.py

from .auth import router as auth_router
from .exams import router as exams_router
from .results import router as results_router
from .schools import router as schools_router
from .students import router as students_router
from .student_subjects import router as student_subjects_router
from .exam_divisions import router as exam_divisions_router
from .exam_grades import router as exam_grades_router
from .subjects import router as subjects_router
from .exam_subjects import router as exam_subjects_router
from .user_exams import router as user_exams_router

__all__ = [
    "auth_router",
    "exams_router",
    "results_router",
    "schools_router",
    "students_router",
    "student_subjects_router",
    "exam_divisions_router",
    "exam_grades_router",
    "subjects_router",
    "exam_subjects_router",
    "user_exams_router",
]