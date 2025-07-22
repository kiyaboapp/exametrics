
from app.db.database import AsyncSession, get_db, init_db
from mama import get_student_subjects_by_centre_and_exam
import json
import asyncio
from utils.pdf.isal import AsyncAttendancePDFGenerator



async def main():
    await init_db()
    
    exam_id = "1f0656e3-8756-680b-ac24-8d5b3e217521"
    centre_number = "s0112"
    ministry = "PO - REGIONAL ADMINISTRATION AND LOCAL GOVERNMENT"
    report_name = "INDIVIDUAL ATTENDANCE LIST"
    
    try:
        db_gen = get_db()
        db = await db_gen.__anext__()
        
        try:
            subject_data, school_info = await get_student_subjects_by_centre_and_exam(
                db, 
                centre_number=centre_number,
                exam_id=exam_id,
                ministry=ministry,
                report_name=report_name
            )

            print("==================================================================")
            pdf_gen = AsyncAttendancePDFGenerator()
            pdf_path = await pdf_gen.generate_pdf(
                school_info=school_info,
                subjects_data=subject_data,
                include_score=True,
                underscore_mode=True,
                separate_every=10
            )
        
            print(f"Generated PDF at: {pdf_path}")
            print("==================================================================")

        except Exception as e:
            with open("student_subjects.json", "w") as f:
                json.dump({"error": str(e)}, f)
        finally:
            await db.close()
    except Exception as e:
        with open("student_subjects.json", "w") as f:
            json.dump({"error": str(e)}, f)
    finally:
        await db_gen.aclose()

if __name__ == "__main__":
    asyncio.run(main())