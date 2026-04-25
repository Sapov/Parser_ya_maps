# db/services/organisation_service.py
from typing import List, Dict, Optional
from contextlib import contextmanager
from db.session_manager import SessionManager
from db.repositories.organisation_repo import OrganisationRepository
from db.repositories.city_repo import CityRepository
from db.repositories.category_repo import CategoryRepository
import logging

logger = logging.getLogger(__name__)


class OrganisationService:
    """Сервис для работы с организациями"""

    def __init__(self, session_manager: SessionManager):
        self.session_manager = session_manager
        self.city_repo = None
        self.category_repo = None
        self.org_repo = None

    @contextmanager
    def _get_repositories(self):
        """Контекстный менеджер для работы с репозиториями"""
        session = self.session_manager.get_sync_session()
        try:
            self.org_repo = OrganisationRepository(session)
            self.city_repo = CityRepository(session)
            self.category_repo = CategoryRepository(session)
            yield
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Ошибка: {e}")
            raise
        finally:
            session.close()

    def add_or_update_organisation(self, items_link: Dict) -> None:
        """Добавить или обновить организацию"""
        with self._get_repositories():
            category = self.category_repo.get_or_create(items_link.get("category"))
            city = self.city_repo.get_or_create(items_link.get("city"))

            existing = self.org_repo.get_by_link(items_link.get("link"))

            if existing:
                existing.title = items_link.get("title")
                existing.rating_yandex = items_link.get("rating_yandex")
                existing.estimation = items_link.get("estimation")
                existing.category = category
                existing.city = city
                logger.info(f"Обновлена организация с ID: {existing.id}")
            else:
                org = Organisations(
                    link=items_link.get("link"),
                    title=items_link.get("title"),
                    rating_yandex=items_link.get("rating_yandex"),
                    estimation=items_link.get("estimation"),
                    category=category,
                    city=city,
                )
                self.org_repo.add(org)
                logger.info(f"Добавлена новая организация с ID: {org.id}")

    def add_full_organisation(self, items_link: Dict) -> None:
        """Добавить полную информацию об организации"""
        with self._get_repositories():
            category = self.category_repo.get_or_create(items_link.get("category"))
            city = self.city_repo.get_or_create(items_link.get("city"))

            org = Organisations(
                link=items_link.get("link"),
                title=items_link.get("title"),
                rating_yandex=items_link.get("rating_yandex"),
                estimation=items_link.get("estimation"),
                phone=items_link.get("phone"),
                address=items_link.get("address"),
                site=items_link.get("site"),
                category=category,
                city=city,
            )
            self.org_repo.add(org)
            logger.info(f"Добавлена организация с ID: {org.id}")

    def update_organisation_details(self, items_link: Dict) -> None:
        """Обновить детали организации (телефон, адрес, сайт)"""
        with self._get_repositories():
            self.org_repo.update_organisation(
                items_link.get("id"),
                {
                    "phone": items_link.get("phone"),
                    "address": items_link.get("address"),
                    "site": items_link.get("site")
                }
            )

    def bulk_add_organisations(self, items: List[Dict]) -> None:
        """Массовое добавление организаций"""
        with self._get_repositories():
            for item in items:
                category = self.category_repo.get_or_create(item.get("category"))
                city = self.city_repo.get_or_create(item.get("city"))

                existing = self.org_repo.get_by_link(item.get("link"))

                if not existing:
                    org = Organisations(
                        link=item.get("link"),
                        title=item.get("title"),
                        rating_yandex=item.get("rating_yandex"),
                        estimation=item.get("estimation"),
                        category=category,
                        city=city,
                    )
                    self.org_repo.add(org)

            logger.info(f"Массово добавлено {len(items)} организаций")

    def get_organisations_by_city_with_email(self, city: str) -> List[Organisations]:
        """Получить организации по городу с email"""
        with self._get_repositories():
            return self.org_repo.get_by_city_with_email(city)

    def get_statistics(self, city: str = None, category: str = None) -> Dict:
        """Получить статистику по организациям"""
        with self._get_repositories():
            if city:
                orgs = self.org_repo.get_by_city(city)
                orgs_with_email = self.org_repo.get_by_city_with_email(city)
            elif category:
                orgs = self.category_repo.get_by_name(category).organisations
                orgs_with_email = self.org_repo.get_by_category_with_email(category)
            else:
                orgs = self.org_repo.get_all()
                orgs_with_email = [o for o in orgs if o.mail]

            return {
                "total": len(orgs),
                "with_email": len(orgs_with_email),
                "percentage": (len(orgs_with_email) / len(orgs) * 100) if orgs else 0
            }