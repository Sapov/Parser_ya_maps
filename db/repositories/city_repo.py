from typing import Dict
from parser.models.city import City
from parser.models.city_all import CityAll
from .base import BaseRepository


class CityRepository(BaseRepository[City]):
    """Репозиторий для работы с городами"""

    def get_model(self):
        return City

    def get_or_create(self, city_name: str) -> City:
        """Получить или создать город"""
        city = self.session.query(City).filter_by(city=city_name).first()
        if not city:
            city = City(city=city_name)
            self.session.add(city)
            self.session.flush()
            logger.info(f"Создан новый город: {city_name}")
        return city

    def get_by_name(self, city_name: str) -> City:
        """Получить город по имени"""
        return self.session.query(City).filter_by(city=city_name).first()


class CityAllRepository(BaseRepository[CityAll]):
    """Репозиторий для работы со всеми городами"""

    def get_model(self):
        return CityAll

    def add_from_dict(self, city_dict: Dict) -> CityAll:
        """Добавить город из словаря"""
        city = CityAll(**city_dict)
        self.session.add(city)
        self.session.flush()
        logger.info(f"Добавлен город: {city.city}")
        return city
