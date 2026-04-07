import os
from pathlib import Path

from pydantic_settings import BaseSettings

# Получаем абсолютный путь к корню проекта
BASE_DIR = Path(__file__).resolve().parent.parent  # поднимаемся на 2 уровня вверх


class Settings(BaseSettings):
    db_url: str = f"sqlite:///{BASE_DIR}/db.sqlite3"

    async_bd_url: str = f"sqlite+aiosqlite:///{BASE_DIR}/db.sqlite3"
    db_echo: bool = False
    app_name: str = "Parser Yandex Map"

    # Celery
    CELERY_BROKER_URL: str = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
    CELERY_RESULT_BACKEND: str = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")

    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/1")


class Config:
    env_file = ".env"


settings = Settings()
