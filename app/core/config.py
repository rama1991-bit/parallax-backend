from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    ENV: str = "development"
    DEBUG: bool = False
    SECRET_KEY: str = "change-me"
    DATABASE_URL: str = "sqlite+aiosqlite:///./parallax.db"
    DATABASE_SSLMODE: str | None = None
    FRONTEND_URL: str = "http://localhost:3000"
    BACKEND_CORS_ORIGINS: str = ""
    ADMIN_API_KEY: str | None = None
    AI_PROVIDER: str = "heuristic"
    OPENAI_API_KEY: str | None = None
    OPENAI_MODEL: str = "gpt-4o-mini"
    OPENAI_BASE_URL: str = "https://api.openai.com/v1"
    RETRIEVAL_PROVIDER: str = "mock"
    EXTERNAL_RETRIEVAL_ENABLED: bool = False
    OPS_NOTIFICATIONS_ENABLED: bool = False
    OPS_WEBHOOK_URL: str | None = None
    OPS_WEBHOOK_SECRET: str | None = None
    OPS_NOTIFICATION_MIN_SEVERITY: str = "warning"
    OPS_NOTIFICATION_TIMEOUT_SECONDS: float = 5.0
    ARTICLE_FETCH_TIMEOUT_SECONDS: float = 15.0
    ARTICLE_MAX_CHARS: int = 12000
    ANALYZE_QUOTA_ENABLED: bool = True
    ANALYZE_DAILY_LIMIT: int = 10
    ANALYZE_QUOTA_WINDOW_SECONDS: int = 86400
    ANALYZE_COOLDOWN_SECONDS: int = 10

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
