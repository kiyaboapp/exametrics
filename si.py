import asyncio
from mama import get_student_subjects_by_centre_and_exam
from utils.pdf.isal import AsyncAttendancePDFGenerator
from app.db.database import AsyncSession

async def main():
    async with AsyncSession() as db:
        subject_data, school_info = await get_student_subjects_by_centre_and_exam(
            db=db,
            centre_number='S0330',
            exam_id="1f0656e3-8756-680b-ac24-8d5b3e217521"
        )
    pdf_gen=AsyncAttendancePDFGenerator()
    pdf_path = await pdf_gen.generate_pdf(
    school_info=school_info,
    subjects_data=subject_data,
    include_score=True,
    underscore_mode=True,
    separate_every=10
    )

    print(f"Generated PDF at: {pdf_path}")

if __name__ == "__main__":
    asyncio.run(main())