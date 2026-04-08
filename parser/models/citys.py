from .base import Base


class Citys(Base):
    number: int  #
    city_name: str  # Город
    region: str  # Регион
    federal_district: str  # Федеральный округ
    population: int  # Население
    foundation_year: int  # Год основания
    city_status_year: int  # Год статуса города
    former_names: str  # Прежние названия
