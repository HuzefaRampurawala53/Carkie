import os
from sqlalchemy import create_engine, text
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    database_url: str

    class Config:
        env_file = ".env"

try:
    settings = Settings()
    print("✅ Loaded .env file successfully.")
except Exception as e:
    print("❌ Error loading .env file. Make sure backend/.env exists and contains DATABASE_URL.")
    print(e)
    exit(1)

print(f"Connecting to database (host: {settings.database_url.split('@')[-1].split('/')[0]})...")
engine = create_engine(settings.database_url)

try:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT postgis_version();"))
        postgis_ver = result.scalar()
        print(f"✅ Connection successful!")
        print(f"📡 PostGIS version: {postgis_ver}")
except Exception as e:
    print("❌ Database connection failed.")
    print(e)
