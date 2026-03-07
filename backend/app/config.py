from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_URL: str = ""  # required: set via .env or environment variable
    SECRET_KEY: str = ""  # required: set via .env or environment variable
    DEBUG: bool = False

    FOF99_USERNAME: str = ""
    FOF99_PASSWORD: str = ""
    FOF99_DEVICE_ID: str = ""

    RISK_FREE_RATE_TYPE: str = "fixed"
    RISK_FREE_RATE_VALUE: float = 0.025

    # CORS origins (comma-separated in .env, e.g. CORS_ORIGINS=http://localhost:3000,https://app.jinfan.com)
    CORS_ORIGINS: str = "http://localhost:3000"

    # File upload limits
    MAX_UPLOAD_SIZE_MB: int = 50

    # Admin bootstrap (required for first user registration)
    ADMIN_SETUP_KEY: str = ""

    # JWT
    JWT_EXPIRE_HOURS: int = 24
    JWT_ALGORITHM: str = "HS256"

    # Redis
    REDIS_URL: str = "redis://localhost:6379/0"
    CACHE_TTL_SECONDS: int = 3600  # 1 hour default cache TTL

    # Celery
    CELERY_BROKER_URL: str = "redis://localhost:6379/1"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/2"

    class Config:
        env_file = ".env"


settings = Settings()

if not settings.DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set. Please configure it in .env or environment variables.")
if not settings.SECRET_KEY:
    raise RuntimeError("SECRET_KEY is not set. Please configure it in .env or environment variables.")
