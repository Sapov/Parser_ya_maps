from typing import List, Optional, Dict
from sqlalchemy import select, and_, func
from parser.models.organisations import Organisations
from parser.models.city import City
from parser.models.category import Category
from .base import BaseRepository
import logging

logger = logging.getLogger(__name__)


class OrganisationRepository(BaseRepository[Organisations]):
    """Репозиторий для работы с организациями"""

    def get_model(self):
        return Organisations

    def get_by_link(self, link: str) -> Optional[Organisations]:
        """Получить организацию по ссылке"""
        return self.session.query(Organisations).filter_by(link=link).first()

    def get_by_category_and_city(
            self,
            category_name: str,
            city_name: str
    ) -> List[Dict]:
        """Получить организации по категории и городу"""
        stmt = select(Organisations).join(
            Category, Organisations.category
        ).join(
            City, Organisations.city
        ).where(
            Category.category == category_name,
            City.city == city_name
        ).order_by(Organisations.rating_yandex.desc())

        result = self.session.execute(stmt)

        return [
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
            for i in result.scalars()
        ]

    def get_filtered_by_rating(self, min_rating: float = 4.0) -> List[Organisations]:
        """Получить организации с рейтингом выше указанного"""
        stmt = select(Organisations).where(
            Organisations.rating_yandex >= str(min_rating)
        )
        return list(self.session.scalars(stmt))

    def get_paginated(
            self,
            page: int = 1,
            per_page: int = 10,
            as_dict: bool = False
    ) -> List:
        """Постраничное получение организаций"""
        stmt = select(Organisations).limit(per_page).offset((page - 1) * per_page)
        result = self.session.scalars(stmt)

        if not as_dict:
            return list(result)

        return [
            {
                "id": i.id,
                "link": i.link,
                "title": i.title,
                "rating_yandex": i.rating_yandex,
                "estimation": i.estimation,
                "phone": i.phone,
                "address": i.address,
                "site": i.site,
                "category": i.category,
            }
            for i in result
        ]

    def update_organisation(self, org_id: int, data: Dict) -> Optional[Organisations]:
        """Обновить организацию"""
        organisation = self.get_by_id(org_id)
        if organisation:
            for key, value in data.items():
                if hasattr(organisation, key) and value is not None:
                    setattr(organisation, key, value)
            self.session.commit()
            logger.info(f"Обновлена организация с ID: {organisation.id}")
        return organisation

    def get_by_city_with_email(self, city: str) -> List[Organisations]:
        """Получить организации в городе с email"""
        query = select(Organisations).join(
            City, Organisations.city_id == City.id
        ).where(
            and_(
                City.city == city,
                Organisations.mail.isnot(None),
                Organisations.mail != ""
            )
        )
        result = self.session.execute(query)
        return result.scalars().all()

    def get_by_city(self, city: str) -> List[Organisations]:
        """Получить все организации в городе"""
        query = select(Organisations).join(
            City, Organisations.city_id == City.id
        ).where(City.city == city)

        result = self.session.execute(query)
        return result.scalars().all()

    def get_by_category_with_email(self, category: str) -> List[Organisations]:
        """Получить организации в категории с email"""
        query = select(Organisations).join(
            Category, Organisations.category_id == Category.id
        ).where(
            and_(
                Category.category == category,
                Organisations.mail.isnot(None),
                Organisations.mail != ""
            )
        )
        result = self.session.execute(query)
        return result.scalars().all()

    def find_duplicates(self, field: str = 'mail') -> List:
        """Найти дубликаты по указанному полю"""
        field_column = getattr(Organisations, field)

        duplicates = self.session.query(
            field_column,
            func.count(Organisations.id).label('count')
        ).group_by(field_column).having(
            func.count(Organisations.id) > 1
        ).all()

        for value, count in duplicates:
            logger.info(f"'{value}' встречается {count} раз")

        return duplicates

