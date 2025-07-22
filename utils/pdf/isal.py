import asyncio
from concurrent.futures import ThreadPoolExecutor
import os
import uuid
from pathlib import Path
from typing import List, Dict, Tuple
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from datetime import datetime
import zipfile
from sqlalchemy import select
from app.db.database import AsyncSession
from app.db.models import StudentSubject, ExamSubject, Student, Exam, School

UPLOAD_DIR = "uploads/download"
os.makedirs(UPLOAD_DIR, exist_ok=True)

# Thread-safe font registration
_fonts_initialized = False

def initialize_fonts():
    global _fonts_initialized
    if not _fonts_initialized:
        try:
            fonts_dir = Path("app/fonts")
            fonts_dir.mkdir(parents=True, exist_ok=True)
            pdfmetrics.registerFont(TTFont("LucidaConsole", str(fonts_dir / "lucon.ttf")))
            pdfmetrics.registerFont(TTFont("DejaVuSans", str(fonts_dir / "DejaVuSans.ttf")))
            _fonts_initialized = True
        except Exception as e:
            raise RuntimeError(f"Failed to initialize fonts: {str(e)}")

initialize_fonts()

# Constants
DEFAULT_FONT = "LucidaConsole"
FALLBACK_FONT = "DejaVuSans"
MAX_WORKERS = 4

# Type aliases
StudentRecord = Tuple[str, str, str]
SubjectData = Dict[str, List[StudentRecord]]
SchoolInfo = Dict[str, str]

async def get_student_subjects_by_centre_and_exam(
    db: AsyncSession, 
    centre_number: str, 
    exam_id: str,
    ministry: str = "PO - REGIONAL ADMINISTRATION AND LOCAL GOVERNMENT",
    report_name: str = "INDIVIDUAL ATTENDANCE LIST",
    subject_filter: str = "All"
) -> tuple[Dict[str, List[Tuple[str, str, str]]], dict]:
    result = {}
    school_info = {}
    
    try:
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

        result_proxy_subjects = await db.execute(stmt_subjects)
        result_proxy_school = await db.execute(stmt_school)
        
        rows = result_proxy_subjects.all()
        if not rows:
            return result, school_info
            
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

        school_row = result_proxy_school.first()
        if school_row:
            school_info = {
                "ministry": ministry.upper(),
                "exam_board": school_row.exam_name.upper(),
                "school_name": f"EXAMINATION CENTRE:{centre_number} - {school_row.school_name.upper()} ({school_row.council_name.upper()}, {school_row.region_name.upper()})",
                "report_name": report_name.upper()
            }
                
    except Exception as e:
        raise
        
    return result, school_info

class AsyncAttendancePDFGenerator:
    """Thread-safe async wrapper for PDF generation"""
    _executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    
    def __init__(self):
        self.temp_dir = UPLOAD_DIR
    
    async def generate_pdf(
        self,
        db: AsyncSession,
        exam_id: str,
        centre_number: str = None,
        region_name: str = None,
        council_name: str = None,
        exam_year: int = None,
        exam_level: str = None,
        output_dir: str = None,
        output_filename: str = None,
        include_score: bool = True,
        underscore_mode: bool = True,
        separate_every: int = 10,
        ministry: str = "PO - REGIONAL ADMINISTRATION AND LOCAL GOVERNMENT",
        report_name: str = "INDIVIDUAL ATTENDANCE LIST",
        subject_filter: str = "All"
    ) -> str:
        """
        Generate PDF(s) asynchronously with thread-safe handling
        Returns path to generated PDF file or ZIP file if multiple PDFs
        """
        output_dir = output_dir or self.temp_dir
        os.makedirs(output_dir, exist_ok=True)

        if centre_number:
            subjects_data, school_info = await get_student_subjects_by_centre_and_exam(
                db, centre_number, exam_id, ministry, report_name, subject_filter
            )
            if not subjects_data:
                return ""
                
            output_filename = output_filename or f"{centre_number}-{exam_year or datetime.now().year}-{exam_level or 'FORM'}-{datetime.now().strftime('%Y%m%d%H%M%S')}.pdf"
            output_path = os.path.join(output_dir, output_filename)
            
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                self._executor,
                self._generate_sync,
                str(output_path),
                school_info,
                subjects_data,
                include_score,
                underscore_mode,
                separate_every
            )
            return str(output_path)
        
        else:
            # Query schools based on region_name and/or council_name
            stmt_schools = select(School.centre_number, School.school_name, School.council_name, School.region_name)
            if region_name and council_name:
                stmt_schools = stmt_schools.where(
                    School.region_name.ilike(f"%{region_name}%"),
                    School.council_name.ilike(f"%{council_name}%")
                )
            elif region_name:
                stmt_schools = stmt_schools.where(School.region_name.ilike(f"%{region_name}%"))
            elif council_name:
                stmt_schools = stmt_schools.where(School.council_name.ilike(f"%{council_name}%"))
            
            result = await db.execute(stmt_schools)
            schools = result.all()
            
            if not schools:
                return ""
                
            pdf_paths = []
            zip_filename = f"attendance_reports_{datetime.now().strftime('%Y%m%d%H%M%S')}.zip"
            zip_path = os.path.join(output_dir, zip_filename)
            
            for school in schools:
                centre_number = school.centre_number
                print(f"    => {school.centre_number}: {school.school_name}")
                subjects_data, school_info = await get_student_subjects_by_centre_and_exam(
                    db, centre_number, exam_id, ministry, report_name, subject_filter
                )
                if not subjects_data:
                    continue
                    
                pdf_filename = f"{centre_number}-{exam_year or datetime.now().year}-{exam_level or 'FORM'}-{datetime.now().strftime('%Y%m%d%H%M%S')}.pdf"
                pdf_path = os.path.join(output_dir, pdf_filename)
                
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(
                    self._executor,
                    self._generate_sync,
                    str(pdf_path),
                    school_info,
                    subjects_data,
                    include_score,
                    underscore_mode,
                    separate_every
                )
                pdf_paths.append(pdf_path)
            
            if len(pdf_paths) == 1:
                return pdf_paths[0]
                
            # Create ZIP file
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for pdf_path in pdf_paths:
                    zipf.write(pdf_path, os.path.basename(pdf_path))
                    os.remove(pdf_path)  # Clean up individual PDFs
            
            return str(zip_path)
    
    @staticmethod
    def _generate_sync(
        filename: str,
        school_info: SchoolInfo,
        subjects_data: SubjectData,
        include_score: bool,
        underscore_mode: bool,
        separate_every: int
    ):
        """Synchronous PDF generation (runs in thread pool)"""
        generator = _AttendancePDFGeneratorInternal(
            filename=filename,
            school_info=school_info,
            subjects_data=subjects_data,
            include_score=include_score,
            underscore_mode=underscore_mode,
            separate_every=separate_every
        )
        generator.generate()

class _AttendancePDFGeneratorInternal:
    def __init__(
        self,
        filename: str,
        school_info: SchoolInfo,
        subjects_data: SubjectData,
        include_score: bool = True,
        underscore_mode: bool = True,
        separate_every: int = 10
    ):
        self.filename = filename
        self.school_info = school_info
        self.subjects_data = subjects_data
        self.include_score = include_score
        self.underscore_mode = underscore_mode
        self.separate_every = separate_every
        self.page_width, self.page_height = A4
        self.x_margin = 20 * mm
        self.y_start = 275 * mm
        self.max_per_page = 40

    def _make_border(self, widths):
        return "+" + "+".join(["-" * w for w in widths]) + "+"

    def _make_sep_border(self, widths):
        return "|" + "+".join(["-" * w for w in widths]) + "|"

    def _make_header(self, headers, widths):
        return "|" + "|".join([h.center(w) for h, w in zip(headers, widths)]) + "|"

    def _format_row(self, data: StudentRecord, widths):
        def center(text, width): return text.center(width)
        def left(text, width): return text.ljust(width)

        signature = "_" * widths[3] if self.underscore_mode else " " * widths[3]
        attendance = center("___", widths[4]) if self.underscore_mode else " " * widths[4]
        score = center("___", widths[5]) if (self.underscore_mode and self.include_score) else " " * widths[5] if self.include_score else None

        row = [
            left(data[0], widths[0]),
            left(data[1], widths[1]),
            center(data[2], widths[2]),
            signature,
            attendance
        ]
        if self.include_score and score is not None:
            row.append(score)
        return "|" + "|".join(row) + "|"

    def _draw_page_header(self, c, y, headers, widths, subject_name):
        c.setFont(DEFAULT_FONT, 11)
        c.drawCentredString(self.page_width / 2, y, self.school_info["ministry"])
        y -= 6 * mm
        c.drawCentredString(self.page_width / 2, y, self.school_info["exam_board"])
        y -= 7 * mm
        c.setFont(DEFAULT_FONT, 10)
        c.drawCentredString(self.page_width / 2, y, self.school_info["school_name"])
        y -= 6 * mm
        c.drawCentredString(self.page_width / 2, y, f"{self.school_info['report_name']} FOR SUBJECT: {subject_name}")
        y -= 8 * mm
        c.setFont(DEFAULT_FONT, 8)
        c.drawString(self.x_margin, y, self._make_border(widths))
        y -= 3.5 * mm
        c.drawString(self.x_margin, y, self._make_header(headers, widths))
        y -= 3.5 * mm
        c.drawString(self.x_margin, y, self._make_border(widths))
        y -= 3.5 * mm
        return y

    def _draw_students_table(self, c, y, students_slice, widths):
        c.setFont(DEFAULT_FONT, 8)
        for i, student in enumerate(students_slice, start=1):
            c.drawString(self.x_margin, y, self._format_row(student, widths))
            y -= 4 * mm
            if i % self.separate_every == 0 and i != len(students_slice):
                c.drawString(self.x_margin, y, self._make_sep_border(widths))
                y -= 4 * mm
        return y

    def _draw_supervisor_declaration(self, c, y):
        lines = [
            "DECLARATION BY SUPERVISOR",
            "I declare that I have personally marked this attendance list and",
            "certify that the ticks (✓) and crosses (✗) are correctly entered.",
            "",
            "________________________________       __________________      ______________",
            "Name and Signature of supervisor           Mobile No.                 Date   "
        ]
        c.setFont(DEFAULT_FONT, 10)
        for i, line in enumerate(lines):
            font = FALLBACK_FONT if i == 2 else DEFAULT_FONT
            c.setFont(font, 10)
            c.drawString(self.x_margin, y, line)
            y -= 7 * mm

    def _draw_page_number(self, c, page_num, total_pages):
        c.setFont(DEFAULT_FONT, 8)
        text = f"Page {page_num} of {total_pages}"
        width = c.stringWidth(text, DEFAULT_FONT, 8)
        c.drawString(self.page_width - self.x_margin - width, 15 * mm, text)

    def generate(self):
        headers = ["INDEX", "CANDIDATE NAME", "SEX", "CAND. SIGNATURE", "ATTENDANCE"]
        widths = [13, 34, 6, 24, 13]
        if self.include_score:
            headers.append("SCORE")
            widths.append(8)

        c = canvas.Canvas(self.filename, pagesize=A4)
        global_page = 1
        total_pages = 0
        for students in self.subjects_data.values():
            total_students = len(students)
            total_pages += (total_students + self.max_per_page - 1) // self.max_per_page

        for subject_name, students in self.subjects_data.items():
            total_students = len(students)
            pages_for_subject = (total_students + self.max_per_page - 1) // self.max_per_page

            for page_num in range(1, pages_for_subject + 1):
                y = self.y_start
                start = (page_num - 1) * self.max_per_page
                end = min(page_num * self.max_per_page, total_students)
                current_students = students[start:end]

                y = self._draw_page_header(c, y, headers, widths, subject_name)
                y = self._draw_students_table(c, y, current_students, widths)
                c.setFont(DEFAULT_FONT, 8)
                c.drawString(self.x_margin, y, self._make_border(widths))
                y -= 10 * mm

                if page_num == pages_for_subject:
                    self._draw_supervisor_declaration(c, y)

                self._draw_page_number(c, global_page, total_pages)
                global_page += 1
                c.showPage()

        c.save()