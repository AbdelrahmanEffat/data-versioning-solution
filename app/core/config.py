from pydantic_settings  import BaseSettings
from typing import ClassVar


class Settings(BaseSettings):
    database_hostname: str
    database_port: str
    database_password: str
    database_name: str
    database_username: str
    secret_key: str
    algorithm: str
    access_token_expire_minutes: int
    PROJECT_NAME: str
    REDIS_URL: ClassVar[str] = 'redis://localhost:6379'
    CACHE_EXPIRATION: ClassVar[int] = 3600  # seconds
    wsl_redis_ip: str 

    class Config:
        env_file = r"C:\Users\secre\OneDrive\Desktop\project-root\.env"

# Instantiate settings
settings = Settings()