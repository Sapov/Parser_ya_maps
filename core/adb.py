import logging
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import select

from parser.models.organisations import Organisations

logger = logging.getLogger(__name__)


class AsyncDB:
    def __init__(self):
        # Для SQLite используем асинхронный драйвер
        self.engine = create_async_engine(
            "sqlite+aiosqlite:////home/sasha/PycharmProjects/Parser_ya_maps/core/db.sqlite3",
            echo=True
        )
        self.async_session = async_sessionmaker(self.engine, class_=AsyncSession, expire_on_commit=False)

    async def insert_data(self, item: dict):
        async with self.async_session() as session:
            try:
                # Асинхронный запрос через select
                result = await session.execute(
                    select(Organisations).where(Organisations.id == item.get("id"))
                )
                entry = result.scalar_one_or_none()

                if entry:
                    entry.mail = item.get("mail")
                    entry.whatsapp = item.get("whatsapp")
                    entry.telegram = item.get("telegram")
                    logger.info(f"Обновлена организация с ID: {entry.id}")
                    await session.commit()
                else:
                    logger.warning(f"Организация с ID {item.get('id')} не найдена")

            except Exception as e:
                await session.rollback()
                logger.error(f"Ошибка: {e}")
                raise
