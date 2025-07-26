from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas
from reportlab.lib import colors
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
import os

# Register a standard font
pdfmetrics.registerFont(TTFont('Times-Bold', 'timesbd.ttf'))
pdfmetrics.registerFont(TTFont('Times-Roman', 'times.ttf'))

# Output file
output_path = "replica_attendance_page1.pdf"

# Initialize canvas
c = canvas.Canvas(output_path, pagesize=A4)
width, height = A4

# Margins
left_margin = 20 * mm
right_margin = 20 * mm
top_margin = 20 * mm

# Draw Headers
c.setFont("Times-Bold", 14)
c.drawCentredString(width / 2, height - top_margin, "MWANZA REGIONAL FORM TWO MOCK EXAMINATION")

c.setFont("Times-Bold", 13)
c.drawCentredString(width / 2, height - top_margin - 18, "INDIVIDUAL SUBJECT ATTENDANCE LIST FOR  2025")

c.setFont("Times-Bold", 12)
c.drawCentredString(width / 2, height - top_margin - 40, "EXAMINATIONS")

# Red pen instructions
c.setFont("Times-Roman", 10)
instruction_text = (
    "Using a red pen, the Supervisor must put a tick (✓)\n"
    "for those candidates who are present and a cross (✗)\n"
    "for those who are absent under the Attendance column."
)
text_obj = c.beginText(left_margin, height - top_margin - 60)
text_obj.setFont("Times-Roman", 10)
for line in instruction_text.split("\n"):
    text_obj.textLine(line)
c.drawText(text_obj)

# Examination centre line
centre_text = "EXAMINATION CENTRE: S5788 - BUKOKWA SS (BUCHOSA, MWANZA)"
c.setFont("Times-Bold", 11)
c.drawString(left_margin, height - top_margin - 105, centre_text)

# Table column headers
table_top = height - top_margin - 130
c.setFont("Times-Bold", 10)
column_titles = ["INDEX", "SEX", "CAND. SIGNATURE", "CANDIDATE NAME", "ATTENDANCE", "SCORE"]
col_x = [left_margin, 55*mm, 65*mm, 105*mm, 170*mm, 190*mm]

for i, title in enumerate(column_titles):
    c.drawString(col_x[i], table_top, title)

# Subject title
c.setFont("Times-Bold", 11)
c.drawString(left_margin, table_top + 12, "011 - CIVICS")

# Student rows
students = [
    "S5788-0001 F ABIGAEL OLANDO SAMSON",
    "S5788-0002 F ADELA DIONIZ LONIZ",
    "S5788-0003 F ADELA IBHUZE ALOYCE",
    "S5788-0004 F ADELA SIAJALI FELESIAN",
    "S5788-0005 F AGAPE FABIAN PASTORI",
    "S5788-0006 F AGNES SAYI TIZIRUKWA",
    "S5788-0007 F AGNESS FELECIAN JUSTINE",
    "S5788-0008 F AISHA ROBERT SHOMARI",
    "S5788-0009 F ALICE FOCAS MULIGA",
    "S5788-0010 F ALICE MAJUTO SULTANI",
    "S5788-0011 F AMINA BANDOMA HAIKA",
    "S5788-0012 F AMINA EDNA MIRAJI",
    "S5788-0013 F ANASTAZIA JOHN DAUD",
    "S5788-0014 F ANASTAZIA MARIA COSTANTINE",
    "S5788-0015 F ANASTAZIA NICOLAUS NESTOLI",
    "S5788-0016 F ANASTAZIA PASCHAL PUNGUJA",
    "S5788-0017 F ANASTAZIA SHILINDE TITO",
    "S5788-0018 F ANETH ANDREA GATAWA",
    "S5788-0019 F ANETH LETISIA LENARD",
    "S5788-0020 F ANETH SAMI MARCO",
    "S5788-0021 F ANETH SAYI VENANCE",
    "S5788-0022 F ANETH TABU HATARI",
    "S5788-0023 F ANGELINA KULWA JOHN",
    "S5788-0024 F ANITHA PILI MALAKI",
    "S5788-0025 F ANOLA BAHATI BILAHI",
    "S5788-0026 F ASHA RAMADHANI KASIMU",
    "S5788-0027 F ASHURA ROZA SYLIVESTER",
    "S5788-0028 F ASTERIA METHOD JOSEPH",
    "S5788-0029 F AVELINA MGINI SIYAJALI",
    "S5788-0030 F AVELINA PETRO EVERIST",
    "S5788-0031 F BEATRICE MPIMA MISAMBO",
    "S5788-0032 F BENADETHA JOHN GULE",
    "S5788-0033 F BERNADETA THEONEST STANSLAUS",
    "S5788-0034 F BETRIDA LEONARD KABISANGA",
    "S5788-0035 F CATHERINE YUGA ABEL",
    "S5788-0036 F CHRISTINA MUSOMA GEJERA",
    "S5788-0037 F CHRISTINA NGUTEI HEZRON",
    "S5788-0038 F CHRISTINA SHIJA THOMAS",
    "S5788-0039 F DAFROZA MAKINGA ANDREA",
    "S5788-0040 F DEBORA MAGENI MAKOYE"
]

row_y = table_top - 15
line_height = 12

c.setFont("Times-Roman", 9)
for student in students:
    parts = student.split(" ", 2)  # INDEX, SEX, NAME
    if len(parts) == 3:
        c.drawString(col_x[0], row_y, parts[0])
        c.drawString(col_x[1], row_y, parts[1])
        c.drawString(col_x[3], row_y, parts[2])
    # empty placeholders for signature, attendance, score
    c.line(col_x[2], row_y - 2, col_x[2] + 35, row_y - 2)  # Signature line
    c.rect(col_x[4], row_y - 8, 12, 10)  # Attendance box
    c.rect(col_x[5], row_y - 8, 12, 10)  # Score box
    row_y -= line_height

# Footer
c.setFont("Times-Roman", 9)
c.drawCentredString(width / 2, 20 * mm, "Page 1 of 74")

# Finalize PDF
c.showPage()
c.save()

print(f"PDF created successfully: {output_path}")
