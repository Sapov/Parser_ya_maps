import os

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    db_url: str = "sqlite:////home/sasha/PycharmProjects/Parser_ya_maps/core/db.sqlite3"
    async_bd_url: str = "sqlite+aiosqlite:////home/sasha/PycharmProjects/Parser_ya_maps/core/db.sqlite3"
    db_echo: bool = False
    app_name: str = "FastAPI with Celery"

    # Celery
    CELERY_BROKER_URL: str = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
    CELERY_RESULT_BACKEND: str = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")

    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/1")


class Config:
    env_file = ".env"


settings = Settings()
