
from .region_service import create_region, get_region, get_regions
from .council_service import create_council, get_council, get_councils
from .ward_service import create_ward, get_ward, get_wards
from .school_service import create_school, get_school, get_schools
from .examination_board_service import create_exam_board, get_exam_board, get_exam_boards
from .exam_service import create_exam, get_exam, get_exams
from .exam_division_service import create_exam_division, get_exam_division, get_exam_divisions
from .exam_grade_service import create_exam_grade, get_exam_grade, get_exam_grades
from .exam_subject_service import create_exam_subject, get_exam_subject, get_exam_subjects
from .subject_service import create_subject, get_subject, get_subjects
from .student_service import create_student, get_student, get_students
from .result_service import create_result, get_result, get_results
from .student_subject_service import create_student_subject, get_student_subject, get_student_subjects
from .user_service import create_user, get_user, get_users
from .user_exam_service import create_user_exam, get_user_exam, get_user_exams

__all__ = [
    "create_region", "get_region", "get_regions",
    "create_council", "get_council", "get_councils",
    "create_ward", "get_ward", "get_wards",
    "create_school", "get_school", "get_schools",
    "create_exam_board", "get_exam_board", "get_exam_boards",
    "create_exam", "get_exam", "get_exams",
    "create_exam_division", "get_exam_division", "get_exam_divisions",
    "create_exam_grade", "get_exam_grade", "get_exam_grades",
    "create_exam_subject", "get_exam_subject", "get_exam_subjects",
    "create_subject", "get_subject", "get_subjects",
    "create_student", "get_student", "get_students",
    "create_result", "get_result", "get_results",
    "create_student_subject", "get_student_subject", "get_student_subjects",
    "create_user", "get_user", "get_users",
    "create_user_exam", "get_user_exam", "get_user_exams",
]
