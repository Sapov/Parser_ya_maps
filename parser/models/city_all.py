# from .base import Base
# from pydantic import BaseModel
#
#
# class CityP(BaseModel):
#     number: int  #
#     city_name: str  # Город
#     region: str  # Регион
#     federal_district: str  # Федеральный округ
#     population: int  # Население
#     foundation_year: int  # Год основания
#     city_status_year: int  # Год статуса города
#     former_names: str  # Прежние названия
#
#
#
# class CityAll(Base):
#     number: int  #
#     city_name: str  # Город
#     region: str  # Регион
#     federal_district: str  # Федеральный округ
#     population: int  # Население
#     foundation_year: int  # Год основания
#     city_status_year: int  # Год статуса города
#     former_names: str  # Прежние названия
#
#
#     def __repr__(self):
#         return f'{self.city_name}|{self.region}|{self.population}'