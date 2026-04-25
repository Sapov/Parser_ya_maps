import os
from pathlib import Path
from typing import ClassVar

from pydantic_settings import BaseSettings

# Получаем абсолютный путь к корню проекта
BASE_DIR = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    db_url: str = f"sqlite:///{BASE_DIR}/db.sqlite3"

    async_bd_url: str = f"sqlite+aiosqlite:///{BASE_DIR}/db.sqlite3"
    db_echo: bool = False
    app_name: str = "Parser Yandex Map"

    # # PostgreSQL настройки
    # DB_USER:str = os.getenv("DB_USER", "postgres")
    # DB_PASSWORD:str = os.getenv("DB_PASSWORD", "postgres")
    # DB_HOST:str = os.getenv("DB_HOST", "localhost")
    # DB_PORT:str = os.getenv("DB_PORT", "5432")
    # DB_NAME:str = os.getenv("DB_NAME", "parser_db")
    # # Формируем URL для PostgreSQL
    # db_url: str = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    # # Асинхронный URL для PostgreSQL
    # async_bd_url: str = f"postgresql+asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    # # Дополнительные настройки
    # db_echo: bool = False
    # db_pool_size: int = int(os.getenv("DB_POOL_SIZE", "20"))
    # db_max_overflow: int = int(os.getenv("DB_MAX_OVERFLOW", "10"))
    # db_pool_timeout: int = int(os.getenv("DB_POOL_TIMEOUT", "30"))
    # db_pool_recycle: int = int(os.getenv("DB_POOL_RECYCLE", "3600"))


    # Celery
    CELERY_BROKER_URL: str = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
    CELERY_RESULT_BACKEND: str = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/1")


class Config:
    env_file = ".env"


settings = Settings()
