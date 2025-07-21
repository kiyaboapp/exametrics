
import asyncio
import json
from typing import Dict, List, Tuple
from sqlalchemy import select
from app.db.database import AsyncSession, get_db, init_db
from app.db.models import StudentSubject, ExamSubject, Student

async def get_student_subjects_by_centre_and_exam(
    db: AsyncSession, 
    centre_number: str, 
    exam_id: str
) -> Dict[str, List[Tuple[str, str, str]]]:
    """
    Returns a dictionary mapping subject codes to lists of student data tuples.
    Splits subjects with has_practical=True into theory (code/1) and practical (code/2).
    """
    result = {}
    
    try:
        stmt = (
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

        result_proxy = await db.execute(stmt)
        rows = result_proxy.all()
        
        if not rows:
            return result
            
        for row in rows:
            try:
                subject_code = row.subject_code
                subject_name = row.subject_name.upper()
                has_practical = row.has_practical
                
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
                
    except Exception as e:
        raise
        
    return result

async def main():
    await init_db()
    
    exam_id = "1f0656e3-8756-680b-ac24-8d5b3e217521"
    centre_number = "S0477"
    
    try:
        db_gen = get_db()
        db = await db_gen.__anext__()
        
        try:
            result = await get_student_subjects_by_centre_and_exam(
                db, 
                centre_number=centre_number,
                exam_id=exam_id
            )
            # print(result)
            
            with open("student_subjects.json", "w") as f:
                json.dump(result, f, indent=2)
                
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
