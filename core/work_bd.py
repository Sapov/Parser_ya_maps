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
        '''
        пройти по всем запиясям и где строка в почте с запятой разбить на две записи
        :return:
        '''

        session = self.Session()
        subquery = session.query(
            func.min(model.id)
        ).group_by(model.mail).scalar_subquery()

        delete_stmt = select(model).where(
            and_(
                model.id.notin_(subquery),
                # Дополнительные условия, если нужно
            )
        )

        result =session.execute(delete_stmt)
        print(len(list(result.scalars())))
        return list(result.scalars())

        session.commit()

if __name__ == '__main__':
    db = DB()
    print(db.dublicates(Organisations, 'mail'))