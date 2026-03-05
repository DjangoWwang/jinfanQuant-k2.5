from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_URL: str = "postgresql+asyncpg://fof_user:jinfan2026@localhost:5432/fof_platform"
    SECRET_KEY: str = "dev-secret-key"
    DEBUG: bool = True

    FOF99_USERNAME: str = ""
    FOF99_PASSWORD: str = ""
    FOF99_DEVICE_ID: str = ""

    RISK_FREE_RATE_TYPE: str = "fixed"
    RISK_FREE_RATE_VALUE: float = 0.025

    class Config:
        env_file = ".env"


settings = Settings()
