from sqlalchemy import create_engine, select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import sessionmaker
from parser.models import Base
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
        # Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)

        async_engine = create_async_engine("sqlite+aiosqlite:////home/sasha/PycharmProjects/Parser_ya_maps/core/db.sqlite3")
        self.async_session = async_sessionmaker(bind=async_engine, expire_on_commit=False)



    async def insert_data(self, item: dict):
        async with self.async_session() as session:
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


    def add_items_link(self, items_link: dict) -> None:
        session = self.Session()
        try:
            # Получаем или создаем категорию
            category_name = items_link.get("category")
            category = session.query(Category).filter_by(category=category_name).first()
            if not category:
                category = Category(category=category_name)
                session.add(category)
                session.flush()  # Получаем ID новой категории
                logger.info(f"Создана новая категория: {category_name}")
            else:
                logger.info(f"Категория уже существует: {category_name}")

            # Получаем или создаем город
            city_name = items_link.get("city")
            city = session.query(City).filter_by(city=city_name).first()
            if not city:
                city = City(city=city_name)
                session.add(city)
                session.flush()  # Получаем ID нового города
                logger.info(f"Создан новый город: {city_name}")
            else:
                logger.info(f"Город уже существует: {city_name}")

            # Проверяем существование организации
            existing_org = session.query(Organisations).filter_by(
                link=items_link.get("link")
            ).first()

            if existing_org:
                # Обновляем существующую запись
                existing_org.title = items_link.get("title")
                existing_org.rating_yandex = items_link.get("rating_yandex")
                existing_org.estimation = items_link.get("estimation")
                existing_org.category = category
                existing_org.city = city
                logger.info(f"Обновлена организация с ID: {existing_org.id}")
            else:
                # Создаем новую запись
                item = Organisations(
                    link=items_link.get("link"),
                    title=items_link.get("title"),
                    rating_yandex=items_link.get("rating_yandex"),
                    estimation=items_link.get("estimation"),
                    category=category,
                    city=city,
                )
                session.add(item)
                logger.info(f"Добавлена новая организация")
                # Сохраняем item, чтобы получить его ID после коммита
                session.flush()
                logger.info(f"Добавлен элемент с ID: {item.id}")

            # ОДИН commit для всех изменений
            session.commit()
            logger.info("Транзакция успешно завершена")

        except Exception as e:
            session.rollback()
            logger.error(f"Ошибка при добавлении/обновлении: {e}")
            raise  # Переподнимаем исключение для диагностики
        finally:
            session.close()

    def get_all_links(self) -> list[Organisations]:
        """Получить все записи из таблицы"""
        session = self.Session()
        try:
            stmt = select(Organisations)
            result = session.execute(stmt)
            return list(result.scalars())
        finally:
            session.close()

    def get_links_filtered(self, min_rating: float = 4.0) -> list[Organisations]:
        """Получить записи с рейтингом выше указанного"""
        session = self.Session()
        try:
            stmt = select(Organisations).where(
                Organisations.rating_yandex >= str(min_rating)
            )
            return list(session.scalars(stmt))
        finally:
            session.close()

    def get_link_by_id(self, link_id: int) -> Organisations | None:
        """Получить одну запись по ID"""
        session = self.Session()
        try:
            return session.get(Organisations, link_id)
        finally:
            session.close()

    def get_links_paginated(
            self, page: int = 1, per_page: int = 10
    ) -> list[Organisations]:
        """Постраничное чтение"""
        session = self.Session()
        try:
            stmt = select(Organisations).limit(per_page).offset((page - 1) * per_page)
            return list(session.scalars(stmt))
        finally:
            session.close()

    def get_links_paginated_up(
            self, page: int = 1, per_page: int = 10
    ) -> list[Organisations]:
        """Постраничное чтение"""
        session = self.Session()
        try:
            stmt = select(Organisations).limit(per_page).offset((page - 1) * per_page)
            lst = []
            for i in list(session.scalars(stmt)):
                lst.append(
                    {
                        "id": i.id,
                        "link": i.link,
                        "title": i.title,
                        "rating_yandex": i.rating_yandex,
                        "estimation": i.estimation,
                        "phone": i.phone,
                        "address": i.address,
                        "site": i.site,
                        "category": i.category,  # "mail":i.mail,
                    }
                )
            return lst
        finally:
            session.close()

    def add_items_organisations(self, items_link: dict) -> None:
        # добавляем организации с сайтом и тел
        session = self.Session()
        try:
            item = Organisations(
                link=items_link.get("link"),
                title=items_link.get("title"),
                rating_yandex=items_link.get("rating_yandex"),
                estimation=items_link.get("estimation"),
                phone=items_link.get("phone"),
                address=items_link.get("address"),
                site=items_link.get("site"),
                category=items_link.get("category"),
                city=items_link.get("city"),
            )
            session.add(item)
            session.commit()
            # Печатаем ДО коммита или используем expire_on_commit=False
            logger.info(f"Добавлена организация с ID: {item.id}")
        except Exception as e:
            session.rollback()
            print(f"Ошибка: {e}")
            raise  # Переподнимаем исключение для диагностики
        finally:
            session.close()

    def get_all_sites(self) -> list[Organisations]:
        """Получить все записи из таблицы"""
        session = self.Session()
        try:
            stmt = select(Organisations)
            result = session.execute(stmt)
            lst = []
            for i in list(result.scalars()):
                lst.append(
                    {
                        "id": i.id,
                        "link": i.link,
                        "title": i.title,
                        "rating_yandex": i.rating_yandex,
                        "estimation": i.estimation,
                        "phone": i.phone,
                        "address": i.address,
                        "site": i.site,
                    }
                )
            return lst

        finally:
            session.close()

    def update_record(self, items_link: dict):
        """
        обновляем запись добавляя тел, адрес, сайт
        """
        with Session(self.engine) as session:
            entry = session.query(Organisations).get(items_link.get("id"))
            if entry:
                entry.phone = items_link.get("phone")
                entry.address = items_link.get("address")
                entry.site = items_link.get("site")

                logger.info(f"Обновлена организация с ID: {entry.id}")
            session.commit()

    def add_items_batch(self, items: list[dict]) -> None:
        """Массовая вставка элементов в БД"""
        session = self.Session()
        try:
            for item in items:
                # Получаем или создаем категорию
                category = session.query(Category).filter_by(category=item.get("category")).first()
                if not category:
                    category = Category(category=item.get("category"))
                    session.add(category)
                    session.flush()

                # Получаем или создаем город
                city = session.query(City).filter_by(city=item.get("city")).first()
                if not city:
                    city = City(city=item.get("city"))
                    session.add(city)
                    session.flush()

                # Проверяем существование организации
                existing = session.query(Organisations).filter_by(link=item.get("link")).first()

                if not existing:
                    org = Organisations(
                        link=item.get("link"),
                        title=item.get("title"),
                        rating_yandex=item.get("rating_yandex"),
                        estimation=item.get("estimation"),
                        category=category,
                        city=city,
                    )
                    session.add(org)

            session.commit()
            logger.info(f"Массово добавлено {len(items)} организаций")
        except Exception as e:
            session.rollback()
            logger.error(f"Ошибка массовой вставки: {e}")
            raise
        finally:
            session.close()


# Проверка работы
if __name__ == "__main__":
    db = DB()
    itm = {'link': 'https://yandex.ru/maps/org/prints/1659610427/', 'title': 'Принц', 'rating_yandex': '5,0',
           'estimation': None, 'city': 'Воронеж', 'category': 'Бассейны'}
    db.add_items_link(itm)
