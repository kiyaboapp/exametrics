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

UPLOAD_DIR = "uploads/download"
os.makedirs(UPLOAD_DIR, exist_ok=True)

# Thread-safe font registration (done once at module level)
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

class AsyncAttendancePDFGenerator:
    """Thread-safe async wrapper for PDF generation"""
    _executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    
    def __init__(self):
        self.temp_dir = UPLOAD_DIR
    
    async def generate_pdf(
        self,
        school_info: SchoolInfo,
        subjects_data: SubjectData,
        include_score: bool = True,
        underscore_mode: bool = True,
        separate_every: int = 10
    ) -> str:
        """
        Generate PDF asynchronously with thread-safe handling
        Returns path to generated PDF file
        """
        output_filename = f"ISAL-BY-KIYABO-APP-{uuid.uuid4().hex}.pdf"
        output_path = os.path.join(UPLOAD_DIR, output_filename)
        
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
    
    async def generate_pdf_with_naming(
        self,
        school_info: SchoolInfo,
        subjects_data: SubjectData,
        centre_number: str,
        exam_year: int,
        exam_level: str,
        output_dir: str,
        include_score: bool = True,
        underscore_mode: bool = True,
        separate_every: int = 10
    ) -> str:
        """
        Generate PDF with custom naming convention and output directory
        Returns path to generated PDF file
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        output_filename = f"{centre_number}-{exam_year}-{exam_level}-{timestamp}.pdf"
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
    """
    Internal synchronous PDF generator
    Maintains all original logic exactly as provided
    """
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
        return "|" + "|".joint(row) + "|"

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
            "______________________________                         ____________________",
            "Name and Signature of supervisor           " + " " * 25 + "Date."
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

# Example usage in FastAPI route
async def example_fastapi_usage():
    pdf_gen = AsyncAttendancePDFGenerator()
    
    subject_data = {
        "011 - CIVICS": [(f"S5788-{i:04d}", f"STUDENT {i} CIVIC", "F" if i % 2 == 0 else "M") for i in range(1, 134)],
        "012 - HISTORY": [(f"S5788-{i+100:04d}", f"STUDENT {i} HIST", "M" if i % 2 == 0 else "F") for i in range(1, 187)]
    }

    school_info = {
        "ministry": "PO - REGIONAL ADMINISTRATION AND LOCAL GOVERNMENT",
        "exam_board": "LAKE ZONE FORM IV MOCK EXAMINATION, JULY 2025",
        "school_name": "EXAMINATION CENTRE: S5788 - BUKOKWA SS (BUCHOSA, MWANZA)",
        "report_name": "INDIVIDUAL ATTENDANCE"
    }

    pdf_path = await pdf_gen.generate_pdf(
        school_info=school_info,
        subjects_data=subject_data,
        include_score=True,
        underscore_mode=True,
        separate_every=10
    )
    
    print(f"Generated PDF at: {pdf_path}")

if __name__ == "__main__":
    asyncio.run(example_fastapi_usage())