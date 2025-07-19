from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.v1 import (
    auth, 
    exams, 
    results, 
    schools, 
    students, 
    student_subjects, 
    exam_divisions, 
    exam_grades,
    exam_subjects,
    subjects,
    user_exams

)
from app.core.config import settings
from app.db.database import init_db

app = FastAPI(
    title="Exametrics API",
    description="API for managing exam results and processing",
    version="1.0.0"
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
app.include_router(auth.router, prefix="/api/v1/auth", tags=["Authentication"])
app.include_router(exams.router, prefix="/api/v1/exams", tags=["Exams"])
app.include_router(results.router, prefix="/api/v1/results", tags=["Results"])
app.include_router(schools.router, prefix="/api/v1/schools", tags=["Schools"])
app.include_router(students.router, prefix="/api/v1/students", tags=["Students"])
app.include_router(student_subjects.router, prefix="/api/v1/student/subjects", tags=["Student Subjects"])
app.include_router(exam_divisions.router, prefix="/api/v1/exam/divisions", tags=["Exam Divisions"])
app.include_router(exam_grades.router, prefix="/api/v1/exam/grades", tags=["Exam Grades"])
app.include_router(exam_subjects.router,prefix="/api/v1/exam/subjects",tags=["Exam Subjects"])
app.include_router(subjects.router,prefix="/api/v1/subjecrs",tags=["Subjects"])
app.include_router(user_exams.router,prefix="/api/v1/exam/users",tags=["User Exams"])

@app.on_event("startup")
async def startup_event():
    await init_db()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)