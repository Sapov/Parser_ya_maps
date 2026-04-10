import json
import time
from os import write

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

from core.db import DB
from parser.models.city_all import CityP


def parse_cities_selenium():
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

        # Даём странице время на полную загрузку JavaScript (особенно для сортируемых таблиц)
        time.sleep(3)

        # 4. Поиск таблицы по уникальному классу или заголовку
        # Таблица с городами находится внутри элемента с классом "wikitable"
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

    return cities_data


def load_in_base(lst_city: list):
    new_city = DB()
    for i in lst_city:
        itm = CityP(**i)
        new_city.add_all_city(itm.dict())


if __name__ == "__main__":
    cities = parse_cities_selenium()
    load_in_base(cities)
    print(f"Всего распарсено c википедии {len(cities)} городов: ")
