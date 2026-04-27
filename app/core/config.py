from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    ENV: str = "development"
    SECRET_KEY: str = "change-me"
    DATABASE_URL: str = "sqlite+aiosqlite:///./parallax.db"
    FRONTEND_URL: str = "http://localhost:3000"
    AI_PROVIDER: str = "mock"
    RETRIEVAL_PROVIDER: str = "mock"
    EXTERNAL_RETRIEVAL_ENABLED: bool = False

    class Config:
        env_file = ".env"


settings = Settings()
