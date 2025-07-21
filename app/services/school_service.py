from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import text
from fastapi import HTTPException
from app.db.models.school import School as SchoolModel
from app.db.models.subject import Subject
from app.db.models.exam_subject import ExamSubject
from app.db.models.student import Student
from app.db.models.student_subject import StudentSubject
from app.db.models.exam import Exam
from app.db.models.region import Region
from app.db.models.council import Council
from app.db.models.ward import Ward
from app.db.schemas.school import SchoolCreate, School
from utils.pdf.pdf_processor import PDFTableProcessor
from utils.pdf.batch_processor import BatchPDFProcessor
from uuid6 import uuid6
import pandas as pd
import re
from typing import List
import logging

async def resolve_location_data(db: AsyncSession, school_data: dict) -> dict:
    rn = school_data.get("region_name")
    cn = school_data.get("council_name")
    wn = school_data.get("ward_name")
    if rn:
        rn = rn.strip().upper()
    if cn:
        cn = cn.strip().upper()
    if wn:
        wn = wn.strip().upper()
    if wn:
        stmt = select(Ward).options(selectinload(Ward.council).selectinload(Council.region)).filter(Ward.ward_name.ilike(wn))
        wards = (await db.execute(stmt)).scalars().all()
        if not wards:
            raise HTTPException(404, f"Ward '{wn}' not found")
        if len(wards) > 1:
            raise HTTPException(400, f"Multiple wards named '{wn}' found. Specify council or ward ID.")
        ward = wards[0]
        school_data["ward_id"] = ward.ward_id
        school_data["council_id"] = ward.council.council_id
        school_data["region_id"] = ward.council.region.region_id
        school_data["ward_name"] = ward.ward_name
        school_data["council_name"] = ward.council.council_name
        school_data["region_name"] = ward.council.region.region_name
    elif cn:
        stmt = select(Council).options(selectinload(Council.region)).filter(Council.council_name.ilike(cn))
        councils = (await db.execute(stmt)).scalars().all()
        if not councils:
            raise HTTPException(404, f"Council '{cn}' not found")
        if len(councils) > 1:
            raise HTTPException(400, f"Multiple councils named '{cn}' found. Specify region.")
        council = councils[0]
        school_data["council_id"] = council.council_id
        school_data["region_id"] = council.region.region_id
        school_data["council_name"] = council.council_name
        school_data["region_name"] = council.region.region_name
    elif rn:
        stmt = select(Region).filter(Region.region_name.ilike(rn))
        region = (await db.execute(stmt)).scalars().first()
        if not region:
            raise HTTPException(404, f"Region '{rn}' not found")
        school_data["region_id"] = region.region_id
        school_data["region_name"] = region.region_name
    else:
        if "ward_id" in school_data:
            stmt = select(Ward).options(selectinload(Ward.council).selectinload(Council.region)).filter(Ward.ward_id == school_data["ward_id"])
            ward = (await db.execute(stmt)).scalars().first()
            if not ward:
                raise HTTPException(404, "Ward ID not found")
            school_data["ward_name"] = ward.ward_name
            school_data["council_id"] = ward.council.council_id
            school_data["council_name"] = ward.council.council_name
            school_data["region_id"] = ward.council.region.region_id
            school_data["region_name"] = ward.council.region.region_name
        elif "council_id" in school_data:
            stmt = select(Council).options(selectinload(Council.region)).filter(Council.council_id == school_data["council_id"])
            council = (await db.execute(stmt)).scalars().first()
            if not council:
                raise HTTPException(404, "Council ID not found")
            school_data["council_name"] = council.council_name
            school_data["region_id"] = council.region.region_id
            school_data["region_name"] = council.region.region_name
        elif "region_id" in school_data:
            region = await db.get(Region, school_data["region_id"])
            if not region:
                raise HTTPException(404, "Region ID not found")
            school_data["region_name"] = region.region_name
    return school_data

async def create_school(db: AsyncSession, school: SchoolCreate) -> School:
    result = await db.execute(select(SchoolModel).filter(SchoolModel.centre_number == school.centre_number))
    if result.scalars().first():
        raise HTTPException(status_code=409, detail="School already exists with this centre number")
    school_data = school.model_dump()
    school_data = await resolve_location_data(db, school_data)
    db_school = SchoolModel(**school_data)
    db.add(db_school)
    await db.commit()
    await db.refresh(db_school)
    return School.model_validate(db_school)

async def get_school(db: AsyncSession, centre_number: str) -> School:
    result = await db.execute(select(SchoolModel).filter(SchoolModel.centre_number == centre_number))
    school = result.scalars().first()
    if not school:
        raise HTTPException(status_code=404, detail="School not found")
    return School.model_validate(school)

async def get_schools(db: AsyncSession, skip: int = 0, limit: int = 100) -> list[School]:
    result = await db.execute(select(SchoolModel).offset(skip).limit(limit))
    return [School.model_validate(school) for school in result.scalars().all()]

def abbreviate_subject(subject_name: str) -> str:
    words = subject_name.split()
    if len(words) == 1:
        return words[0][:3].upper()
    return ''.join(word[0].upper() for word in words if word.lower() not in ['in', 'language'])

async def process_pdf_data(db: AsyncSession, pdf_path: str, exam_id: str) -> None:
    pdf_data = PDFTableProcessor.parse_pdf_to_data(pdf_path)
    school_info = pdf_data['school_info']
    student_data = pdf_data['student_data']
    subject_data = pdf_data['subject_data']
    centre_number = school_info['CENTRE_NUMBER']
    result = await db.execute(select(SchoolModel).filter(SchoolModel.centre_number == centre_number))
    school = result.scalars().first()
    if school:
        school.school_name = school_info['SCHOOL_NAME']
        school.school_type = school_info['SCHOOL_TYPE']
    else:
        school_data = {
            "centre_number": centre_number,
            "school_name": school_info['SCHOOL_NAME'],
            "school_type": school_info['SCHOOL_TYPE']
        }
        school_data = await resolve_location_data(db, school_data)
        school = SchoolModel(**school_data)
        db.add(school)
    await db.commit()
    await db.refresh(school)
    for _, row in subject_data.iterrows():
        subject_code = row['CODE']
        subject_name = row['SUBJECT']
        result = await db.execute(select(Subject).filter(Subject.subject_code == subject_code))
        subject = result.scalars().first()
        if not subject:
            subject_short = abbreviate_subject(subject_name)
            subject = Subject(
                subject_code=subject_code,
                subject_name=subject_name,
                subject_short=subject_short,
                has_practical=False,
                exclude_from_gpa=False
            )
            db.add(subject)
            await db.commit()
            await db.refresh(subject)
        result = await db.execute(select(ExamSubject).filter(ExamSubject.exam_id == exam_id, ExamSubject.subject_code == subject_code))
        exam_subject = result.scalars().first()
        if not exam_subject:
            exam_subject = ExamSubject(
                exam_id=exam_id,
                subject_code=subject_code,
                subject_name=subject.subject_name,
                subject_short=subject.subject_short,
                has_practical=subject.has_practical,
                exclude_from_gpa=subject.exclude_from_gpa
            )
            db.add(exam_subject)
    await db.commit()
    for _, row in student_data.iterrows():
        student_id = row['CANDIDATE']
        result = await db.execute(select(Student).filter(Student.exam_id == exam_id, Student.student_id == student_id, Student.centre_number == centre_number))
        student = result.scalars().first()
        if not student:
            student = Student(
                student_global_id=str(uuid6()),
                exam_id=exam_id,
                student_id=student_id,
                centre_number=centre_number,
                first_name=row['FIRST_NAME'],
                middle_name=row['MIDDLE_NAME'],
                surname=row['LAST_NAME'],
                sex=row['SEX']
            )
            db.add(student)
        subject_codes = [col for col in student_data.columns if re.match(r'^\d{3}$', col)]
        for code in subject_codes:
            if pd.notna(row[code]):
                result = await db.execute(select(StudentSubject).filter(StudentSubject.exam_id == exam_id, StudentSubject.student_global_id == student.student_global_id, StudentSubject.centre_number == centre_number, StudentSubject.subject_code == code))
                student_subject = result.scalars().first()
                if not student_subject:
                    student_subject = StudentSubject(
                        id=str(uuid6()),
                        exam_id=exam_id,
                        student_global_id=student.student_global_id,
                        centre_number=centre_number,
                        subject_code=code
                    )
                    db.add(student_subject)
    await db.commit()

async def process_batch_pdf_data(db: AsyncSession, pdf_paths: List[str], exam_id: str) -> None:
    result = await db.execute(select(Exam).filter(Exam.exam_id == exam_id))
    exam = result.scalars().first()
    if not exam:
        raise HTTPException(status_code=400, detail=f"Exam ID {exam_id} not found")
    
    students_df, report_df = BatchPDFProcessor.process_pdf_files(pdf_paths, split_names=False)
    
    required_columns = ['CENTRE NUMBER', 'SCHOOL NAME', 'SCHOOL TYPE']
    missing_columns = [col for col in required_columns if col not in report_df.columns]
    if missing_columns:
        raise HTTPException(status_code=500, detail=f"Missing columns in report_df: {missing_columns}")
    
    required_student_columns = ['CANDIDATE', 'FULL NAME', 'SEX', 'CENTRE_NUMBER']
    missing_student_columns = [col for col in required_student_columns if col not in students_df.columns]
    if missing_student_columns:
        raise HTTPException(status_code=500, detail=f"Missing columns in students_df: {missing_student_columns}")
    
    valid_centres = set(report_df['CENTRE NUMBER'])
    student_centres = set(students_df['CENTRE_NUMBER'])
    invalid_centres = student_centres - valid_centres
    if invalid_centres:
        raise HTTPException(status_code=400, detail=f"Invalid CENTRE_NUMBER values in students_df: {invalid_centres}")
    
    for _, school_row in report_df.iterrows():
        centre_number = school_row.get('CENTRE NUMBER', 'UNKNOWN_CENTRE')
        school_name = school_row.get('SCHOOL NAME', 'UNKNOWN_SCHOOL')
        school_type = school_row.get('SCHOOL TYPE', 'UNKNOWN')
        result = await db.execute(select(SchoolModel).filter(SchoolModel.centre_number == centre_number))
        school = result.scalars().first()
        if school:
            school.school_name = school_name
            school.school_type = school_type
        else:
            school_data = {
                "centre_number": centre_number,
                "school_name": school_name,
                "school_type": school_type
            }
            try:
                school_data = await resolve_location_data(db, school_data)
                school = SchoolModel(**school_data)
                db.add(school)
            except HTTPException:
                continue
    
    await db.commit()
    
    subject_codes = [col for col in students_df.columns if re.match(r'^\d{3}$', col)]
    with db.no_autoflush:
        for code in subject_codes:
            subject_name = f"SUBJECT {code}"
            result = await db.execute(select(Subject).filter(Subject.subject_code == code))
            subject = result.scalars().first()
            if not subject:
                subject_short = abbreviate_subject(subject_name)
                subject = Subject(
                    subject_code=code,
                    subject_name=subject_name,
                    subject_short=subject_short,
                    has_practical=False,
                    exclude_from_gpa=False
                )
                db.add(subject)
                await db.commit()
                await db.refresh(subject)
            result = await db.execute(select(ExamSubject).filter(ExamSubject.exam_id == exam_id, ExamSubject.subject_code == code))
            exam_subject = result.scalars().first()
            if not exam_subject:
                exam_subject = ExamSubject(
                    exam_id=exam_id,
                    subject_code=code,
                    subject_name=subject.subject_name,
                    subject_short=subject.subject_short,
                    has_practical=subject.has_practical if subject.has_practical is not None else False,
                    exclude_from_gpa=subject.exclude_from_gpa if subject.exclude_from_gpa is not None else False
                )
                db.add(exam_subject)
    
    await db.commit()
    
    existing_students = {}
    result = await db.execute(
        select(Student.student_id, Student.centre_number, Student.student_global_id)
        .filter(Student.exam_id == exam_id, Student.centre_number.in_(student_centres))
    )
    for row in result.fetchall():
        existing_students[(row.student_id, row.centre_number)] = row.student_global_id
    
    students_to_add = []
    student_global_ids = {}
    
    for _, row in students_df.iterrows():
        student_id = row['CANDIDATE']
        centre_number = row['CENTRE_NUMBER']
        if (student_id, centre_number) not in existing_students:
            full_name = row['FULL NAME'].strip().split()
            if len(full_name) >= 3:
                first_name = full_name[0]
                surname = full_name[-1]
                middle_name = ' '.join(full_name[1:-1]) or None
            elif len(full_name) == 2:
                first_name = full_name[0]
                middle_name = None
                surname = full_name[1]
            else:
                first_name = full_name[0]
                middle_name = None
                surname = None
            student_global_id = str(uuid6())
            students_to_add.append(
                Student(
                    student_global_id=student_global_id,
                    exam_id=exam_id,
                    student_id=student_id,
                    centre_number=centre_number,
                    first_name=first_name,
                    middle_name=middle_name,
                    surname=surname or "-",
                    sex=row['SEX']
                )
            )
            student_global_ids[(student_id, centre_number)] = student_global_id
        else:
            logging.info(f"Skipping duplicate student in pre-check: {student_id} in centre {centre_number}")
            student_global_ids[(student_id, centre_number)] = existing_students[(student_id, centre_number)]
    
    if students_to_add:
        student_params = [
            {
                "student_global_id": s.student_global_id,
                "exam_id": s.exam_id,
                "student_id": s.student_id,
                "centre_number": s.centre_number,
                "first_name": s.first_name,
                "middle_name": s.middle_name,
                "surname": s.surname,
                "sex": s.sex
            }
            for s in students_to_add
        ]
        result = await db.execute(
            text("""
                INSERT IGNORE INTO students
                (student_global_id, exam_id, student_id, centre_number, first_name, middle_name, surname, sex)
                VALUES (:student_global_id, :exam_id, :student_id, :centre_number, :first_name, :middle_name, :surname, :sex)
            """),
            student_params
        )
        await db.commit()
        inserted_rows = result.rowcount
        expected_rows = len(students_to_add)
        if inserted_rows < expected_rows:
            logging.info(f"Skipped {expected_rows - inserted_rows} duplicate students during bulk insert")

    existing_student_subjects = set()
    result = await db.execute(
        select(StudentSubject.student_global_id, StudentSubject.subject_code)
        .filter(StudentSubject.exam_id == exam_id, StudentSubject.centre_number.in_(student_centres))
    )
    for row in result.fetchall():
        existing_student_subjects.add((row.student_global_id, row.subject_code))
    
    student_subjects_to_add = []
    for _, row in students_df.iterrows():
        student_id = row['CANDIDATE']
        centre_number = row['CENTRE_NUMBER']
        student_global_id = student_global_ids.get((student_id, centre_number))
        if student_global_id:
            for code in subject_codes:
                if pd.notna(row[code]) and (student_global_id, code) not in existing_student_subjects:
                    student_subjects_to_add.append(
                        StudentSubject(
                            id=str(uuid6()),
                            exam_id=exam_id,
                            student_global_id=student_global_id,
                            centre_number=centre_number,
                            subject_code=code
                        )
                    )
    
    if student_subjects_to_add:
        student_subject_params = [
            {
                "id": s.id,
                "exam_id": s.exam_id,
                "student_global_id": s.student_global_id,
                "centre_number": s.centre_number,
                "subject_code": s.subject_code
            }
            for s in student_subjects_to_add
        ]
        result = await db.execute(
            text("""
                INSERT IGNORE INTO student_subjects
                (id, exam_id, student_global_id, centre_number, subject_code)
                VALUES (:id, :exam_id, :student_global_id, :centre_number, :subject_code)
            """),
            student_subject_params
        )
        await db.commit()
        inserted_rows = result.rowcount
        expected_rows = len(student_subjects_to_add)
        if inserted_rows < expected_rows:
            logging.info(f"Skipped {expected_rows - inserted_rows} duplicate student subjects during bulk insert")