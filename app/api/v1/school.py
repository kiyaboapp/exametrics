from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from sqlalchemy.ext.asyncio import AsyncSession
from app.api.deps import get_current_user, get_db
from app.db.models.user import User
from app.db.schemas.school import SchoolCreate, School
from app.services.school_service import create_school, get_school, get_schools, process_pdf_data, process_batch_pdf_data
from typing import List
import zipfile
import io
from pathlib import Path
import shutil
import uuid
import logging

router = APIRouter(prefix="/schools", tags=["schools"])

@router.post("/", response_model=School)
async def create_school_endpoint(school: SchoolCreate, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await create_school(db, school)

@router.get("/{centre_number}", response_model=School)
async def get_school_endpoint(centre_number: str, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await get_school(db, centre_number)

@router.get("/", response_model=List[School])
async def get_schools_endpoint(skip: int = 0, limit: int = 100, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    return await get_schools(db, skip, limit)

@router.post("/upload/pdf", response_model=dict)
async def upload_single_pdf(file: UploadFile = File(...), exam_id: str = None, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    if not file.filename.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="File must be a PDF")
    if file.size > 150 * 1024:  # 150KB
        raise HTTPException(status_code=400, detail="File size must not exceed 150KB")
    
    # Create uploads/school directory
    upload_dir = Path("uploads/school")
    upload_dir.mkdir(parents=True, exist_ok=True)
    
    # Create unique temporary subdirectory
    temp_dir = upload_dir / str(uuid.uuid4())
    temp_dir.mkdir(exist_ok=True)
    
    # Save file temporarily
    temp_path = temp_dir / file.filename
    try:
        with open(temp_path, "wb") as buffer:
            buffer.write(await file.read())
        
        await process_pdf_data(db, str(temp_path), exam_id)
        return {"message": f"PDF {file.filename} processed successfully"}
    finally:
        # Clean up
        if temp_dir.exists():
            shutil.rmtree(temp_dir)

@router.post("/upload/bulk-pdf", response_model=dict)
async def upload_bulk_pdf(files: List[UploadFile] = File(...), exam_id: str = None, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    if len(files) > 100:
        raise HTTPException(status_code=400, detail="Cannot process more than 100 files at once")
    
    total_size = sum(file.size for file in files)
    if total_size > 100 * 1024 * 1024:  # 100MB
        raise HTTPException(status_code=400, detail="Total file size must not exceed 100MB")
    
    # Create uploads/school directory
    upload_dir = Path("uploads/school")
    upload_dir.mkdir(parents=True, exist_ok=True)
    
    # Create unique temporary subdirectory
    temp_dir = upload_dir / str(uuid.uuid4())
    temp_dir.mkdir(exist_ok=True)
    
    pdf_paths = []
    try:
        for file in files:
            if not file.filename.lower().endswith('.pdf'):
                raise HTTPException(status_code=400, detail=f"File {file.filename} must be a PDF")
            if file.size > 150 * 1024:  # 150KB
                raise HTTPException(status_code=400, detail=f"File {file.filename} must not exceed 150KB")
            
            temp_path = temp_dir / file.filename
            with open(temp_path, "wb") as buffer:
                buffer.write(await file.read())
            pdf_paths.append(str(temp_path))
        
        # Process in chunks of 20
        chunk_size = 20
        for i in range(0, len(pdf_paths), chunk_size):
            chunk = pdf_paths[i:i + chunk_size]
            await process_batch_pdf_data(db, chunk, exam_id)
        
        return {"message": f"{len(pdf_paths)} PDFs processed successfully"}
    finally:
        # Clean up
        if temp_dir.exists():
            shutil.rmtree(temp_dir)

@router.post("/upload/zip", response_model=dict)
async def upload_zip(file: UploadFile = File(...), exam_id: str = None, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    if not file.filename.lower().endswith('.zip'):
        raise HTTPException(status_code=400, detail="File must be a ZIP")
    if file.size > 100 * 1024 * 1024:  # 100MB
        raise HTTPException(status_code=400, detail="ZIP file must not exceed 100MB")
    
    # Create uploads/school directory
    upload_dir = Path("uploads/school")
    upload_dir.mkdir(parents=True, exist_ok=True)
    
    # Create unique temporary subdirectory
    temp_dir = upload_dir / str(uuid.uuid4())
    temp_dir.mkdir(exist_ok=True)
    
    try:
        # Read and unzip file
        zip_data = await file.read()
        pdf_paths = []
        with zipfile.ZipFile(io.BytesIO(zip_data), 'r') as zip_ref:
            pdf_files = [f for f in zip_ref.namelist() if f.lower().endswith('.pdf')]
            if len(pdf_files) > 100:
                raise HTTPException(status_code=400, detail="ZIP contains more than 100 PDFs")
            
            zip_ref.extractall(temp_dir)
            for pdf_file in pdf_files:
                pdf_path = temp_dir / pdf_file
                if pdf_path.stat().st_size > 150 * 1024:  # 150KB
                    raise HTTPException(status_code=400, detail=f"PDF {pdf_file} must not exceed 150KB")
                pdf_paths.append(str(pdf_path))
        
        # Process in chunks of 20
        chunk_size = 20
        for i in range(0, len(pdf_paths), chunk_size):
            chunk = pdf_paths[i:i + chunk_size]
            await process_batch_pdf_data(db, chunk, exam_id)
        
        return {"message": f"{len(pdf_paths)} PDFs from ZIP processed successfully"}
    finally:
        # Clean up
        if temp_dir.exists():
            shutil.rmtree(temp_dir)