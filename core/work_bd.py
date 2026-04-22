from sqlalchemy import select, and_, or_, func, delete

from sqlalchemy import create_engine, select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import sessionmaker
from parser.models import Base
from parser.models.city_all import CityAll
# from parser.models.city_all import CityAll
from parser.models.organisations import Organisations
from parser.models.city import City
from parser.models.category import Category
from sqlalchemy.orm import Session
import logging
from core.config import settings

logger = logging.getLogger(__name__)


class DB:
    def __init__(self):
        self.engine = create_engine(
            url=settings.db_url,
            echo=settings.db_echo,
        )
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)
        Base.metadata.create_all(self.engine)
        async_engine = create_async_engine(settings.async_bd_url)
        self.async_session = async_sessionmaker(bind=async_engine, expire_on_commit=False)

    def dublicates(self, model, field:str):

        session = self.Session()

        """
        Удаляет дубликаты, оставляя запись с минимальным ID
        """
        # Подзапрос с минимальными ID для каждого значения поля
        subquery = session.query(
            func.min(model.id).label('min_id')
        ).group_by(
            getattr(model, field)
        ).subquery()

        duplicates = session.query(model).filter(
            model.id.notin_(subquery)
        ).all()

        print(f"Найдено дубликатов: {len(duplicates)}")
        for dup in duplicates:
            print(f"ID: {dup.id}, {field}: {getattr(dup, field)}")

        # Удаляем всё, что не входит в подзапрос
        # deleted_count = session.query(model).filter(
        #     model.id.notin_(session.query(subquery.c.min_id))
        # ).delete(synchronize_session=False)
        #
        # session.commit()
        # return deleted_count



if __name__ == '__main__':
    db = DB()
    print(db.dublicates(Organisations, 'mail'))