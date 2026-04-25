from parser import parser_city_in_wiki
from db.session_manager import SessionManager
from db.services.organisation_service import OrganisationService
import logging

logger = logging.getLogger(__name__)


class Database:
    """Фасад для работы с базой данных"""

    def __init__(self):
        self.session_manager = SessionManager()
        self.session_manager.create_table()

        # Инициализация сервисов
        self.organisations = OrganisationService(self.session_manager)

        # Загрузка городов из wiki
        parser_city_in_wiki.LoadAllCity().run()

    def get_all_links(self):
        """Получить все ссылки (для совместимости)"""
        with self.organisations._get_repositories():
            return self.organisations.org_repo.get_all()

    def get_city(self):
        """Получить все города (для совместимости)"""
        from db.repositories.city_repo import CityRepository
        with self.organisations._get_repositories():
            repo = CityRepository(self.organisations.org_repo.session)
            return repo.get_all()

    def get_by_category_and_city(self, category_name: str, city_name: str):
        """Получить организации по категории и городу"""
        with self.organisations._get_repositories():
            return self.organisations.org_repo.get_by_category_and_city(
                category_name, city_name
            )

    def get_all_sites(self):
        """Получить все сайты (для совместимости)"""
        with self.organisations._get_repositories():
            orgs = self.organisations.org_repo.get_all()
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
                for i in orgs
            ]

    def find_duplicates(self):
        """Найти дубликаты"""
        with self.organisations._get_repositories():
            return self.organisations.org_repo.find_duplicates('mail')

    # Делегирование методов сервису
    def add_items_link(self, items_link: dict):
        return self.organisations.add_or_update_organisation(items_link)

    def add_items_organisations(self, items_link: dict):
        return self.organisations.add_full_organisation(items_link)

    def update_record(self, items_link: dict):
        return self.organisations.update_organisation_details(items_link)

    def add_items_batch(self, items: list):
        return self.organisations.bulk_add_organisations(items)

    def city_select_with_email(self, city: str):
        return self.organisations.get_organisations_by_city_with_email(city)


# Использование
if __name__ == "__main__":
    db = Database()
    print(db.find_duplicates())
    stats = db.organisations.get_statistics(city="Москва")
    print(f"Статистика по Москве: {stats}")
