import asyncio
from concurrent.futures import ThreadPoolExecutor
import os
import uuid
from pathlib import Path
from typing import List, Dict, Tuple
import pyodbc
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

# Base directory for output
BASE_OUTPUT_DIR = Path("C:/Users/droge/OneDrive/Desktop/HANDLER/DISTRIBUTIONS/OUTPUTS")
os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)

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
StudentRecord = Tuple[str, str, str]  # (student_id, full_name, sex)
SubjectData = Dict[str, List[StudentRecord]]
SchoolInfo = Dict[str, str]

class AsyncAttendancePDFGenerator:
    _executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    
    def __init__(self):
        self.temp_dir = BASE_OUTPUT_DIR
    
    async def generate_pdf(
        self,
        school_info: SchoolInfo,
        subjects_data: SubjectData,
        output_path: str,
        include_score: bool = True,
        underscore_mode: bool = True,
        separate_every: int = 10
    ) -> str:
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
    
    @staticmethod
    def _generate_sync(
        filename: str,
        school_info: SchoolInfo,
        subjects_data: SubjectData,
        include_score: bool,
        underscore_mode: bool,
        separate_every: int
    ):
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
            "________________________________        ___________________    ________________",
            "Name and Signature of Supervisor            Mobile No.                 Date"
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
        total_pages = sum((len(students) + self.max_per_page - 1) // self.max_per_page 
                         for students in self.subjects_data.values())

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

async def fetch_data_from_access():
    # Connect to MS Access database
    conn_str = (
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        r"DBQ=C:\Users\droge\OneDrive\Desktop\HANDLER\DISTRIBUTIONS\JOINTS\Joint Exams v1.3.1.accdb;"
    )
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Fetch all data in a single query
        cursor.execute("""
            SELECT tbl_students.student_id, tbl_students.sex, tbl_students.full_name, 
                   tbl_student_subject_registration.is_practical, tbl_school_subjects.is_present, 
                   tbl_school_subjects.subject_name, tbl_school_subjects.subject_code, 
                   tbl_school_subjects.has_practical, tbl_school_info.centre_number, 
                   tbl_school_info.school_name, tbl_school_info.ward_name, 
                   tbl_school_info.council_name, tbl_school_info.region_name
            FROM (tbl_school_info INNER JOIN tbl_students ON tbl_school_info.centre_number = tbl_students.centre_number) 
            INNER JOIN (tbl_school_subjects INNER JOIN tbl_student_subject_registration 
                        ON tbl_school_subjects.subject_code = tbl_student_subject_registration.subject_code) 
            ON tbl_students.student_id = tbl_student_subject_registration.student_id
            WHERE tbl_school_subjects.is_present = True
            ORDER BY tbl_school_subjects.subject_code, tbl_students.student_id ASC;
        """)
        student_data = cursor.fetchall()

        cursor.close()
        conn.close()

        return student_data
    except pyodbc.Error as e:
        print(f"Database error: {e}")
        return []

async def generate_all_pdfs():
    pdf_gen = AsyncAttendancePDFGenerator()
    
    # Fetch data
    student_data = await fetch_data_from_access()

    # Organize data by region, council, and centre
    organized_data = {}
    for row in student_data:
        # Check for None or empty values in critical fields
        if not all([row.centre_number, row.school_name]):
            print(f"Warning: Skipping row with missing data - student_id: {row.student_id}, "
                  f"region: {row.region_name}, council: {row.council_name}, centre: {row.centre_number}, "
                  f"school: {row.school_name}, subject_code: {row.subject_code}")
            continue

        # Handle NULL values by setting to 'unknown'
        region = row.region_name.strip() if row.region_name else "unknown"
        council = row.council_name.strip() if row.council_name else "unknown"
        centre = row.centre_number.strip()
        ward = row.ward_name.strip() if row.ward_name else "unknown"

        # Determine subject key based on has_practical and is_practical
        subject_code = row.subject_code.strip()
        subject_name = row.subject_name.strip().upper()
        if row.has_practical:
            if row.is_practical:
                subject_key = f"{subject_code}/2 - {subject_name} (PRACTICAL)"
            else:
                subject_key = f"{subject_code}/1 - {subject_name}"
        else:
            subject_key = f"{subject_code} - {subject_name}"

        student_record = (row.student_id, row.full_name, row.sex)

        # Build nested dictionary
        if region not in organized_data:
            organized_data[region] = {}
        if council not in organized_data[region]:
            organized_data[region][council] = {}
        if centre not in organized_data[region][council]:
            organized_data[region][council][centre] = {}
        if subject_key not in organized_data[region][council][centre]:
            organized_data[region][council][centre][subject_key] = []
        organized_data[region][council][centre][subject_key].append(student_record)

    # Generate PDFs
    for region, councils in organized_data.items():
        region_dir = BASE_OUTPUT_DIR / region
        region_dir.mkdir(exist_ok=True)
        
        for council, centres in councils.items():
            council_dir = region_dir / council
            council_dir.mkdir(exist_ok=True)
            
            for centre, subjects_data in centres.items():
                # Create school_info_dict for this centre
                # Find a representative row for this centre to get school_name, council_name, region_name
                centre_data = next(
                    (row for row in student_data if row.centre_number.strip() == centre),
                    None
                )
                if not centre_data:
                    print(f"Warning: No data found for centre {centre}")
                    continue

                school_info_dict = {
                    "ministry": "PO - REGIONAL ADMINISTRATION AND LOCAL GOVERNMENTs",
                    "exam_board": "MBEYA REGION FORM FOUR MOCK II EXAMINATIONS 2025",
                    "school_name": f"EXAMINATION CENTRE: {centre_data.centre_number} - {centre_data.school_name} ({centre_data.council_name or 'unknown'}, {centre_data.region_name or 'unknown'})",
                    "report_name": "INDIVIDUAL ATTENDANCE LIST"
                }

                output_filename = f"{centre_data.centre_number} - {centre_data.school_name} ISAL.pdf"
                output_path = council_dir / output_filename
                await pdf_gen.generate_pdf(
                    school_info=school_info_dict,
                    subjects_data=subjects_data,
                    output_path=output_path,
                    include_score=True,
                    underscore_mode=True,
                    separate_every=10
                )
                print(f"Generated PDF at: {output_path}")

if __name__ == "__main__":
    asyncio.run(generate_all_pdfs())