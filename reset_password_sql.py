import sys
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from app.db.models.user import User
from app.core.security import get_password_hash
from app.core.config import settings
import asyncio

print(get_password_hash('anon@kiyabo.com'))