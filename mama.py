from typing import Dict, List, Tuple
from sqlalchemy import select
from app.db.database import AsyncSession
from app.db.models import StudentSubject, ExamSubject, Student, Exam, School

async def get_student_subjects_by_centre_and_exam(
    db: AsyncSession, 
    centre_number: str = None,
    exam_id: str = None,
    region_name: str = None,
    council_name: str = None,
    ministry: str = "PO - REGIONAL ADMINISTRATION AND LOCAL GOVERNMENT",
    report_name: str = "INDIVIDUAL ATTENDANCE LIST",
    subject_filter: str = "All"
) -> List[Tuple[Dict[str, List[Tuple[str, str, str]]], dict]]:
    """
    Returns a list of tuples containing:
    1. Dictionary mapping subject codes to lists of student data tuples.
    2. School info dictionary with ministry, exam board, school name, and report name.
    If centre_number is provided, processes for that centre. If region_name and/or council_name are provided,
    processes for all schools in the region/council. Splits subjects with has_practical=True into theory (code/1)
    and practical (code/2) based on subject_filter ('All', 'PracticalOnly', 'TheoryOnly').
    """
    results = []
    
    try:
        # Determine schools to process
        stmt_schools = select(School.centre_number, School.school_name, School.council_name, School.region_name)
        if centre_number:
            stmt_schools = stmt_schools.where(School.centre_number == centre_number)
        elif region_name and council_name:
            stmt_schools = stmt_schools.where(
                School.region_name.ilike(f"%{region_name}%"),
                School.council_name.ilike(f"%{council_name}%")
            )
        elif region_name:
            stmt_schools = stmt_schools.where(School.region_name.ilike(f"%{region_name}%"))
        elif council_name:
            stmt_schools = stmt_schools.where(School.council_name.ilike(f"%{council_name}%"))
        
        school_result = await db.execute(stmt_schools)
        schools = school_result.all()
        
        if not schools:
            return results
        
        i=0
        totals=len(schools)
        for school in schools:
            print(f"{i+1} / {totals} {school.school_name}  ")
            i=+1
            centre_number = school.centre_number
            result = {}
            school_info = {}
            
            # Query for student subjects
            stmt_subjects = (
                select(
                    StudentSubject.subject_code,
                    ExamSubject.subject_name,
                    ExamSubject.has_practical,
                    Student.student_id,
                    Student.full_name,
                    Student.sex
                )
                .join(ExamSubject, StudentSubject.subject_code == ExamSubject.subject_code)
                .join(Student, StudentSubject.student_global_id == Student.student_global_id)
                .where(
                    StudentSubject.centre_number == centre_number,
                    StudentSubject.exam_id == exam_id,
                    ExamSubject.exam_id == exam_id
                )
            )

            # Query for school info
            stmt_school = (
                select(
                    Exam.exam_name,
                    School.school_name,
                    School.council_name,
                    School.region_name
                )
                .join(School, School.centre_number == centre_number)
                .where(
                    School.centre_number == centre_number,
                    Exam.exam_id == exam_id
                )
            )

            # Execute queries
            result_proxy_subjects = await db.execute(stmt_subjects)
            result_proxy_school = await db.execute(stmt_school)
            
            # Process subject data
            rows = result_proxy_subjects.all()
            if not rows:
                continue
                
            for row in rows:
                try:
                    subject_code = row.subject_code
                    subject_name = row.subject_name.upper()
                    has_practical = row.has_practical
                    
                    if subject_filter == "PracticalOnly":
                        if has_practical:
                            practical_key = f"{subject_code}/2 - {subject_name} (PRACTICAL)"
                            practical_data = (row.student_id, row.full_name, row.sex)
                            result.setdefault(practical_key, []).append(practical_data)
                    elif subject_filter == "TheoryOnly":
                        key = f"{subject_code} - {subject_name}"
                        student_data = (row.student_id, row.full_name, row.sex)
                        result.setdefault(key, []).append(student_data)
                    else:  # All
                        if has_practical:
                            theory_key = f"{subject_code}/1 - {subject_name}"
                            theory_data = (row.student_id, row.full_name, row.sex)
                            result.setdefault(theory_key, []).append(theory_data)
                            
                            practical_key = f"{subject_code}/2 - {subject_name} (PRACTICAL)"
                            practical_data = (row.student_id, row.full_name, row.sex)
                            result.setdefault(practical_key, []).append(practical_data)
                        else:
                            key = f"{subject_code} - {subject_name}"
                            student_data = (row.student_id, row.full_name, row.sex)
                            result.setdefault(key, []).append(student_data)
                        
                except Exception as row_error:
                    continue

            # Process school info
            school_row = result_proxy_school.first()
            if school_row:
                school_info = {
                    "ministry": ministry.upper(),
                    "exam_board": school_row.exam_name.upper(),
                    "school_name": f"EXAMINATION CENTRE:{centre_number} - {school_row.school_name.upper()} ({school_row.council_name.upper()}, {school_row.region_name.upper()})",
                    "report_name": report_name.upper()
                }
                
            if result:
                results.append((result, school_info))
                
    except Exception as e:
        raise
        
    return results