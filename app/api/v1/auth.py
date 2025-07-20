
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.ext.asyncio import AsyncSession
from app.api.deps import get_db
from app.db.schemas.user import User
from app.services.user_service import get_user_by_username
from app.core.security import verify_password, create_access_token
from app.core.config import settings
from datetime import timedelta

router = APIRouter(prefix="/auth", tags=["auth"])

@router.get("/")
async def all_endpoints(c):
    return {"message": "All endpoints for authentication",
            "/login": {
                "method": "POST",
                "description": "Login with username and password",
                "request_body": {
                    "username": "string",
                    "password": "string"
                },
                "response": {
                    "access_token": "string",
                    "token_type": "string"
                }
            }
            }

@router.post("/login", response_model=dict)
async def login(form_data: OAuth2PasswordRequestForm = Depends(), db: AsyncSession = Depends(get_db)):
    user = await get_user_by_username(db, form_data.username)
    if not user or not verify_password(form_data.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token = create_access_token(
        data={"sub": user.username},
        expires_delta=timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    )
    return {"access_token": access_token, "token_type": "bearer"}
