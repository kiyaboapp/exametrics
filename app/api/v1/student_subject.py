
from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi.responses import JSONResponse
from app.api.deps import get_current_user, get_db
from app.db.models.user import User
from app.db.schemas.student_subject import StudentSubjectCreate, StudentSubject
from app.services.student_subject_service import create_student_subject, get_student_subject, get_student_subjects
from typing import List
import os
from fastapi.responses import FileResponse
from utils.excel.excel import export_to_excel,import_marks_from_excel_old
from typing import List, Optional
import uuid
from datetime import datetime
from utils.processor.subjects import SubjectProcessor
from app.core.config import Settings



# Configuration
UPLOAD_BASE_DIR = "data/imports/marks"
os.makedirs(UPLOAD_BASE_DIR, exist_ok=True)  # Ensure base directory exists



router = APIRouter(prefix="/student-subjects", tags=["student-subjects"])

@router.post("/", response_model=StudentSubject)
async def create_student_subject_endpoint(student_subject: StudentSubjectCreate, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await create_student_subject(db, student_subject)

@router.get("/{student_subject_id}", response_model=StudentSubject)
async def get_student_subject_endpoint(student_subject_id: str, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await get_student_subject(db, student_subject_id)

@router.get("/", response_model=List[StudentSubject])
async def get_student_subjects_endpoint(skip: int = 0, limit: int = 100, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await get_student_subjects(db, skip, limit)



@router.get("/export/excel", response_description="Path to the exported Excel file")
async def export_excel_endpoint(
    exam_id: str = Query(..., description="Required exam identifier"),
    ward_name: str = Query("", description="Filter by ward name"),
    council_name: str = Query("", description="Filter by council name"),
    region_name: str = Query("", description="Filter by region name"),
    school_type: str = Query("", description="Filter by school type"),
    practical_mode: int = Query(0, description="Practical exam mode flag (0 or 1)"),
    marks_filler: str = Query("", description="Placeholder for missing marks"),
    centre_number: Optional[str] = Query(None, description="Single centre number"),
    centre_number_list: Optional[List[str]] = Query(None, description="List of centre numbers"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Export exam data to Excel with the specified filters.
    
    Either centre_number OR centre_number_list can be provided, but not both.
    """
    # Validate parameters
    if centre_number and centre_number_list:
        raise HTTPException(
            status_code=400,
            detail="Provide either centre_number OR centre_number_list, not both"
        )

    try:
        # Call the imported export function
        file_path = await export_to_excel(
            exam_id=exam_id,
            ward_name=ward_name,
            council_name=council_name,
            region_name=region_name,
            school_type=school_type,
            practical_mode=practical_mode,
            marks_filler=marks_filler,
            centre_number=centre_number or "",
            centre_number_list=centre_number_list or []
        )
        
        if not os.path.exists(file_path):
            raise HTTPException(
                status_code=404,
                detail="Export file not found at the generated path"
            )
            
        return FileResponse(
            path=file_path,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=os.path.basename(file_path)
        )
            
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Export failed: {str(e)}"
        )



@router.post("/import/marks", response_description="Import marks from Excel file")
async def import_marks_endpoint(
    file: UploadFile = File(..., description="Excel file containing marks data"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Import exam marks from an Excel file (saved permanently for reference).
    The file will be stored in: data/imports/marks/YEAR/MONTH/FILENAME
    
    Returns:
    - JSON response with the count of updated records and file path
    """
    try:
        # Create organized directory structure (by year/month)
        today = datetime.now()
        upload_dir = os.path.join(
            UPLOAD_BASE_DIR,
            str(today.year),
            f"{today.month:02d}"
        )
        os.makedirs(upload_dir, exist_ok=True)
        
        # Generate unique filename with timestamp
        original_name = os.path.splitext(file.filename)[0]
        file_ext = os.path.splitext(file.filename)[1]
        save_filename = f"{original_name}_{today.strftime('%Y%m%d_%H%M%S')}{file_ext}"
        save_path = os.path.join(upload_dir, save_filename)
        
        # Save the uploaded file permanently
        with open(save_path, "wb") as buffer:
            buffer.write(await file.read())
        
        # Call import function
        updated_count = await import_marks_from_excel_old(file_path=save_path)
        
        return JSONResponse(
            content={
                "status": "success",
                "updated_records": updated_count,
                "saved_path": save_path,
                "message": "File saved for future reference"
            }
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Import failed: {str(e)}"
        )



@router.post("/process/subject/{exam_id}")
async def process_subject_endpoint(exam_id: str) -> dict:
    """Process subject data for given exam ID using SubjectProcessor"""
    try:
        processor = SubjectProcessor(exam_id=exam_id, settings=Settings())
        return await processor.process_all()
    except Exception as e:
        return {
            "exam_id": exam_id,
            "student_subjects_count": 0,
            "schools_count": 0,
            "exam_subjects_count": 0,
            "updated_records": 0,
            "exported_file": None, 
            "first_row": None,
            "processing_time_seconds": 0.0,
            "success": False,
            "error": str(e)
        }