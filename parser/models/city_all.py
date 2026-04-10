from typing import Optional

from sqlalchemy.orm import Mapper, Mapped

from .base import Base
from pydantic import BaseModel


class CityP(BaseModel):
    number: int  #
    city_name: str  # Город
    region: str  # Регион
    federal_district: str  # Федеральный округ
    population: str  # Население
    foundation_year: str  # Год основания
    city_status_year: str  # Год статуса города
    former_names: Optional[str] # Прежние названия


#
class CityAll(Base):
    number: Mapped[int]  #
    city_name: Mapped[str]  # Город
    region: Mapped[str]  # Регион
    federal_district: Mapped[str]  # Федеральный округ
    population: Mapped[str]  # Население
    foundation_year: Mapped[str]  # Год основания
    city_status_year: Mapped[str]  # Год статуса города
    former_names: Mapped[str]  # Прежние названия


    def __repr__(self):
        return f'{self.city_name}|{self.region}|{self.population}'



