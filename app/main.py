from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from app.api.v1 import (
    region_router, council_router, ward_router, school_router,
    examination_board_router, exam_router, exam_division_router,
    exam_grade_router, exam_subject_router, subject_router,
    student_router, result_router, student_subject_router,
    user_router, user_exam_router, auth_router,isal_router
)
from app.core.config import settings
from app.db.database import init_db

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield

app = FastAPI(
    title="Exametrics API",
    description="API for managing exam results and processing",
    version="1.0.0",
    lifespan=lifespan
)

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routers
app.include_router(region_router, prefix="/api/v1")
app.include_router(council_router, prefix="/api/v1")
app.include_router(ward_router, prefix="/api/v1")
app.include_router(school_router, prefix="/api/v1")
app.include_router(examination_board_router, prefix="/api/v1")
app.include_router(exam_router, prefix="/api/v1")
app.include_router(exam_division_router, prefix="/api/v1")
app.include_router(exam_grade_router, prefix="/api/v1")
app.include_router(exam_subject_router, prefix="/api/v1")
app.include_router(subject_router, prefix="/api/v1")
app.include_router(student_router, prefix="/api/v1")
app.include_router(result_router, prefix="/api/v1")
app.include_router(student_subject_router, prefix="/api/v1")
app.include_router(user_router, prefix="/api/v1")
app.include_router(user_exam_router, prefix="/api/v1")
app.include_router(auth_router, prefix="/api/v1")
app.include_router(isal_router,prefix="/api/v1")

# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8000,reload=True)