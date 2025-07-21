
from fastapi import Depends, HTTPException, Query,APIRouter
from fastapi.responses import FileResponse
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.database import get_db
from app.services.attendance_service import generate_attendance_pdf
from typing import Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/isals",
    tags=["ISALS"]
)

@router.get("/generate-attendance-pdf")
async def generate_attendance_pdf_endpoint(
    exam_id: str = Query(...),
    centre_number: Optional[str] = Query(None),
    ward_name: Optional[str] = Query(None),
    council_name: Optional[str] = Query(None),
    region_name: Optional[str] = Query(None),
    include_score: bool = Query(True),
    underscore_mode: bool = Query(True),
    separate_every: int = Query(10),
    zip_output: bool = Query(False),
    ministry: Optional[str] = Query("PRESIDENT'S OFFICE, REGIONAL ADMINISTRATION AND LOCAL GOVERNMENTS"),
    exam_board: Optional[str] = Query(None),
    report_name: Optional[str] = Query("INDIVIDUAL ATTENDANCE LIST"),
    db: AsyncSession = Depends(get_db)
) -> FileResponse:
    try:
        file_path = await generate_attendance_pdf(
            exam_id=exam_id,
            centre_number=centre_number,
            ward_name=ward_name,
            council_name=council_name,
            region_name=region_name,
            include_score=include_score,
            underscore_mode=underscore_mode,
            separate_every=separate_every,
            zip_output=zip_output,
            ministry=ministry,
            exam_board=exam_board,
            report_name=report_name,
            db=db
        )
        logger.info(f"Generated file: {file_path}")
        return FileResponse(file_path, media_type="application/pdf" if not zip_output else "application/zip")
    except HTTPException as e:
        logger.error(f"HTTP error: {e.detail}")
        raise e
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
