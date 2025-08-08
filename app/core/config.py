# app/core/config.py

from pydantic_settings import BaseSettings
from pydantic import Field
from typing import List
from dotenv import load_dotenv
import os
import urllib.parse

load_dotenv()

def parse_allowed_origins(value: str) -> List[str]:
    if not value:
        return []
    return [origin.strip() for origin in value.split(",") if origin.strip()]

class Settings(BaseSettings):
    PROJECT_NAME: str = Field(..., env="PROJECT_NAME")
    API_V1_STR: str = Field(..., env="API_V1_STR")
    SECRET_KEY: str = Field(..., env="SECRET_KEY")
    ALGORITHM: str = Field(..., env="ALGORITHM")
    ACCESS_TOKEN_EXPIRE_MINUTES: int = Field(..., env="ACCESS_TOKEN_EXPIRE_MINUTES")
    ALLOWED_ORIGINS: str = Field(..., env="ALLOWED_ORIGINS")
    
    # Database settings
    DB_HOST: str = Field(..., env="DB_HOST")
    DB_PORT: int = Field(..., env="DB_PORT")
    DB_USER: str = Field(..., env="DB_USER")
    DB_PASSWORD: str = Field(..., env="DB_PASSWORD")
    DB_NAME: str = Field(..., env="DB_NAME")
    
    # Construct DATABASE_URL with URL-encoded password
    @property
    def DATABASE_URL(self) -> str:
        return f"mysql+aiomysql://{self.DB_USER}:{urllib.parse.quote(self.DB_PASSWORD)}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()