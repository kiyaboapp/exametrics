
import asyncio
import os
import shutil
import logging
from typing import Dict, List, Tuple, Optional
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.sql import and_
from zipfile import ZipFile
from pathlib import Path
from datetime import datetime
from app.db.models import School, StudentSubject, Exam, ExamSubject, Student
from utils.pdf.isal import AsyncAttendancePDFGenerator
from uuid import uuid4

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

UPLOAD_DIR = Path("uploads/download")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

async def generate_attendance_pdf(
    exam_id: str,
    centre_number: Optional[str] = None,
    ward_name: Optional[str] = None,
    council_name: Optional[str] = None,
    region_name: Optional[str] = None,
    include_score: bool = True,
    underscore_mode: bool = True,
    separate_every: int = 10,
    zip_output: bool = False,
    ministry: Optional[str] = "PRESIDENT'S OFFICE, REGIONAL ADMINISTRATION AND LOCAL GOVERNMENTS",
    exam_board: Optional[str] = None,
    report_name: Optional[str] = "INDIVIDUAL ATTENDANCE LIST",
    db: AsyncSession = None
) -> str:
    # Fetch exam details
    exam = (await db.execute(select(Exam).filter(Exam.exam_id == exam_id))).scalars().first()
    if not exam:
        logger.error(f"Exam not found: {exam_id}")
        raise HTTPException(status_code=404, detail="Exam not found")

    exam_year = exam.start_date.year if exam.start_date else datetime.now().year
    exam_level = exam.exam_level or "FORM_IV"
    logger.info(f"Processing exam: {exam_id}, year: {exam_year}, level: {exam_level}")

    # Fetch school details
    school_query = select(School)
    if centre_number:
        school_query = school_query.filter(School.centre_number == centre_number)
    if ward_name:
        school_query = school_query.filter(School.ward_name == ward_name)
    if council_name:
        school_query = school_query.filter(School.council_name == council_name)
    if region_name:
        school_query = school_query.filter(School.region_name == region_name)

    schools = (await db.execute(school_query)).scalars().all()
    if not schools:
        logger.error("No schools found for the given criteria")
        raise HTTPException(status_code=404, detail="No schools found")

    # Fetch subjects for the exam
    subjects = (await db.execute(
        select(ExamSubject).filter(ExamSubject.exam_id == exam_id)
    )).scalars().all()

    if not subjects:
        logger.error(f"No subjects found for exam: {exam_id}")
        raise HTTPException(status_code=404, detail="No subjects found")

    # Initialize PDF generator
    pdf_generator = AsyncAttendancePDFGenerator()

    # Prepare output
    output_files = []
    for school in schools:
        logger.info(f"Processing school: {school.centre_number} - {school.school_name}")
        subjects_data: Dict[str, List[Tuple[str, str, str]]] = {}

        # Fetch student subjects with INNER JOIN to ensure valid student records
        for subject in subjects:
            subject_key = f"{subject.subject_code} - {subject.subject_name}"
            query = (
                select(StudentSubject, Student)
                .join(Student, Student.student_global_id == StudentSubject.student_global_id)
                .filter(
                    and_(
                        StudentSubject.exam_id == exam_id,
                        StudentSubject.centre_number == school.centre_number,
                        StudentSubject.subject_code == subject.subject_code
                    )
                )
            )

            result = (await db.execute(query)).all()
            subject_entries = []
            for student_subject, student in result:
                if not student.full_name or not student.sex:
                    logger.warning(f"Invalid student data for {student.student_global_id}: full_name={student.full_name}, sex={student.sex}")
                    continue
                subject_entries.append((student.student_global_id, student.full_name, student.sex))
            
            subjects_data[subject_key] = subject_entries
            logger.info(f"Subject {subject_key}: {len(subject_entries)} students")

        if not subjects_data:
            logger.warning(f"No valid data for school {school.centre_number}")
            continue

        # Generate PDF
        try:
            school_info = {
                "school_name": school.school_name,
                "centre_number": school.centre_number,
                "region_name": school.region_name,
                "council_name": school.council_name,
                "ward_name": school.ward_name,
                "exam_year": exam_year,
                "exam_level": exam_level,
                "ministry": ministry,
                "exam_board": exam_board or "Unknown",
                "report_name": report_name
            }
            output_path = UPLOAD_DIR / f"{school.centre_number}_{uuid4()}.pdf"
            await pdf_generator.generate_pdf_with_naming(
                subjects_data=subjects_data,
                school_info=school_info,
                output_path=output_path,
                include_score=include_score,
                underscore_mode=underscore_mode,
                separate_every=separate_every
            )
            output_files.append(output_path)
            logger.info(f"Generated PDF for {school.centre_number}: {output_path}")
        except AttributeError as e:
            logger.error(f"Failed to generate PDF for {school.centre_number}: {str(e)}")
            raise HTTPException(status_code=500, detail=f"PDF generation error: {str(e)}")

    if not output_files:
        logger.error("No PDFs generated")
        raise HTTPException(status_code=500, detail="No PDFs generated")

    # Handle single or multiple outputs
    if len(output_files) == 1 and not zip_output:
        return str(output_files[0])

    zip_path = UPLOAD_DIR / f"attendance_{uuid4()}.zip"
    with ZipFile(zip_path, 'w') as zip_file:
        for file in output_files:
            zip_file.write(file, file.name)
            file.unlink()  # Remove individual PDFs
    logger.info(f"Generated ZIP: {zip_path}")
    return str(zip_path)
