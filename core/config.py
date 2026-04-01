from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    db_url: str = "sqlite:////home/sasha/PycharmProjects/ParingYaMaps/parsers/core/db.sqlite3"
    # 'sqlite+aiosqlite://./dbs.sqlite3'
    db_echo: bool = False


settings = Settings()
