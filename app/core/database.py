from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from .config import settings

# Construct the database URL
SQLALCHEMY_DATABASE_URL = f'postgresql+pg8000://{settings.database_username}:{settings.database_password}@{settings.database_hostname}:{settings.database_port}/{settings.database_name}'

# Create an engine instance
engine = create_engine(SQLALCHEMY_DATABASE_URL)

# Configure SessionLocal class for session handling
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for declaring ORM models
Base = declarative_base()

# Dependency function for database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
