from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import sessionmaker

from core.config import settings
from parser.models import Base


class SessionManager:

    def __init__(self):
        self.sync_engine = create_engine(
            url=settings.db_url,
            echo=settings.db_echo,
        )

        self.async_engine = create_async_engine(settings.async_bd_url)

        self.SyncSession = sessionmaker(bind=self.sync_engine, expire_on_commit=False)
        self.AsyncSession = async_sessionmaker(bind=self.async_engine, expire_on_commit=False)


    def get_sync_session(self):
        return self.SyncSession()

    async def get_async_session(self):
        """Получить асинхронную сессию"""
        async with self.AsyncSession() as session:
            yield session

    def create_table(self):
        Base.metadata.create_all(self.sync_engine)

