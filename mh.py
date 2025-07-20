import asyncio
from app.db.database import AsyncSessionLocal
from app.db.schemas.user import UserCreate
from app.services.user_service import create_user

async def test_create_admin_user():
    async with AsyncSessionLocal() as session:
        user_data = {
            "username": "admin",
            "email": "anon@kiyabo.com",
            "first_name": "Anuarite",
            "middle_name": "Nhende",
            "surname": "Kiyabo",
            "password": "admin123",  # required field
            "role": "ADMIN",
            "is_active": True,
            "is_verified": True
            # "centre_number": "s1869"
        }

        try:
            user_create = UserCreate(**user_data)
            result = await create_user(session, user_create)
            print("✅ User created successfully:")
            print(result.model_dump())
        except Exception as e:
            print("❌ Failed to create user:", e)

if __name__ == "__main__":
    asyncio.run(test_create_admin_user())
