import os
from sqlalchemy import create_engine, text
from pydantic_settings import BaseSettings

# Define a settings class to load variables from our .env file automatically
class Settings(BaseSettings):
    database_url: str

    class Config:
        env_file = ".env"

# Load the settings
try:
    settings = Settings()
    print("✅ Loaded .env file successfully.")
except Exception as e:
    print("❌ Error loading .env file. Make sure backend/.env exists and contains DATABASE_URL.")
    print(e)
    exit(1)

# Create a connection engine
print(f"Connecting to database (host: {settings.database_url.split('@')[-1].split('/')[0]})...")
engine = create_engine(settings.database_url)

try:
    # Attempt to connect and run a spatial query
    with engine.connect() as conn:
        # Check PostGIS version
        result = conn.execute(text("SELECT postgis_version();"))
        postgis_ver = result.scalar()
        print(f"✅ Connection successful!")
        print(f"📡 PostGIS version: {postgis_ver}")
except Exception as e:
    print("❌ Database connection failed.")
    print(e)