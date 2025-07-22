from app.db.database import AsyncSession, get_db, init_db
from mama import get_student_subjects_by_centre_and_exam
import json
import asyncio
from utils.pdf.isal import AsyncAttendancePDFGenerator

async def main():
    await init_db()
    
    exam_id = "1f0656e3-8756-680b-ac24-8d5b3e217521"
    council_name = "Mbeya cc"
    ministry = "PO - REGIONAL ADMINISTRATION AND LOCAL GOVERNMENT"
    report_name = "INDIVIDUAL ATTENDANCE LIST"
    
    try:
        db_gen = get_db()
        db = await db_gen.__anext__()
        
        try:
            # results = await get_student_subjects_by_centre_and_exam(
            #     db, 
            #     region_name=region_name,
            #     exam_id=exam_id,
            #     ministry=ministry,
            #     report_name=report_name,
            #     subject_filter="TheoryOnly"
            # )

            print("==================================================================")
            pdf_gen = AsyncAttendancePDFGenerator()
            pdf_path = await pdf_gen.generate_pdf(
                db=db,
                exam_id=exam_id,
                council_name=council_name,
                include_score=True,
                underscore_mode=True,
                separate_every=10,
                ministry=ministry,
                report_name=report_name,
                # subject_filter="TheoryOnly"
            )
        
            print(f"Generated output at: {pdf_path}")
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