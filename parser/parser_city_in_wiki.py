import logging
import time

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from sqlalchemy import inspect

from core.db import DB
from parser.models.city_all import CityP


class LoadAllCity:
    TABLE_NAME='cityall'
    def __init__(self):
        self.db = DB()


    def parse_cities_selenium(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")

        # 2. Запуск драйвера
        driver = webdriver.Chrome(options=chrome_options)
        cities_data = []

        try:
            # 3. Загрузка страницы
            url = "https://ru.wikipedia.org/wiki/%D0%A1%D0%BF%D0%B8%D1%81%D0%BE%D0%BA_%D0%B3%D0%BE%D1%80%D0%BE%D0%B4%D0%BE%D0%B2_%D0%A0%D0%BE%D1%81%D1%81%D0%B8%D0%B8"
            driver.get(url)

            time.sleep(3)

            table = driver.find_element(By.CLASS_NAME, "jquery-tablesorter")

            # 5. Извлечение строк из тела таблицы
            rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")

            # Пропускаем заголовок, если он есть. Заголовок таблицы находится в thead, поэтому в tbody его нет.
            for row in rows:
                # Получаем все ячейки (td) в строке
                cells = row.find_elements(By.TAG_NAME, "td")
                if not cells:
                    continue

                # Извлекаем данные. Колонки: Номер, Герб, Город, Регион, Федеральный округ, Население, Год основания, Год статуса города, Прежние названия
                # Некоторые поля могут быть пустыми, поэтому нужна проверка.
                try:
                    # Первая ячейка с номером может быть убрана визуально, но в HTML она есть
                    number = cells[0].text if len(cells) > 0 else None
                    # Герб — это изображение, пропускаем (cells[1])
                    city_name = cells[2].text if len(cells) > 2 else None
                    region = cells[3].text if len(cells) > 3 else None
                    federal_district = cells[4].text if len(cells) > 4 else None
                    population = cells[5].text if len(cells) > 5 else None
                    foundation_year = cells[6].text if len(cells) > 6 else None
                    city_status_year = cells[7].text if len(cells) > 7 else None
                    former_names = cells[8].text if len(cells) > 8 else None

                    # Сохраняем только строки, где есть название города
                    if city_name:
                        cities_data.append({
                            "number": number,
                            "city_name": city_name,
                            "region": region,
                            "federal_district": federal_district,
                            "population": population,
                            "foundation_year": foundation_year,
                            "city_status_year": city_status_year,
                            "former_names": former_names,
                        })

                except Exception as e:
                    # Если структура строки нарушена, пропускаем её
                    print(f"Ошибка при обработке строки: {e}")
                    continue

        except Exception as e:
            print(f"Произошла ошибка: {e}")
        finally:
            # Закрываем браузер
            driver.quit()
        print(f"Всего распарсено c википедии {len(cities_data)} городов: ")
        logging.info(f"Всего распарсено c википедии {len(cities_data)} городов: ")

        return cities_data

    def load_in_base(self, lst_city: list):

        for i in lst_city:
            itm = CityP(**i)
            self.db.add_all_city(itm.dict())

    def table_exists(self, table_name):
        """Проверка существования таблицы"""
        inspector = inspect(self.db.engine)
        return table_name in inspector.get_table_names()

    def run(self):
        if not self.table_exists(self.TABLE_NAME):
            print(f'Нет таблицы {self.TABLE_NAME} создаем')
            logging.info(f'Нет таблицы {self.TABLE_NAME} создаем')
            all_city = self.parse_cities_selenium()
            self.load_in_base(all_city)
        print(f'Таблица существует ни чего делать не требуется')
        logging.info(f'Таблица существует ни чего делать не требуется')


if __name__ == '__main__':
    LoadAllCity().run()

