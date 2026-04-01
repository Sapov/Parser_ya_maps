import random
import time

import undetected_chromedriver as uc
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

import logging

logger = logging.getLogger(__name__)


class ParserCard:
    URL: str = "https://yandex.ru/maps/193/voronezh/search/"
    VERSION_CHROME: int = 146

    def __init__(self, category: str, location: str, quantity: int):
        self.quantity = quantity
        self.location = location
        self.category = category
        self.driver = None

    def setup_selenium(self):
        options = Options()
        options.add_argument("--headless=new")
        return options

    def __create_full_url(self):
        return f"{self.URL}{self.category} {self.location}"

    def create_web_driver(self):
        self.driver = uc.Chrome(
            version_main=self.VERSION_CHROME, options=self.setup_selenium()
        )

    def __parse_block_page(self):
        self.driver.get(self.__create_full_url())

        """Получаем блоки элементов"""
        preview_count = elements = count = 0
        logger.info(f"[INFO] --Парсим блоки выдачи в яндекс картах--")
        logger.info(f"[INFO] --Категория: {self.category} город: {self.location}--")

        while True if not self.quantity else count <= self.quantity:
            elements = self.driver.find_elements(By.CSS_SELECTOR, ".search-snippet-view")
            """Прокрутка вниз !! строка не терпит форматирования линтерами"""
            self.driver.execute_script("arguments[0].scrollIntoView(true);", elements[-1])
            # self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", elements_new[-1])

            time.sleep(random.randint(1, 7))
            elements = self.driver.find_elements(
                By.CSS_SELECTOR, ".search-snippet-view"
            )
            count = len(elements)
            logger.info(f"Найдено элементов: {count}")
            if preview_count == count:
                break
            preview_count = count
            self.__parser_card(elements)

    def __parser_card(self, elements):
        print(f"""[INFO] Получаем ссылку, название, рейтинг, и количество оценок""")
        for i in elements:
            try:
                link = i.find_element(
                    By.CSS_SELECTOR, ".search-snippet-view .link-overlay"
                ).get_attribute("href")
                print(link)
            except Exception as e:
                logging.info(f"Нет селектора:  ССЫЛКА error: {e}")
                link = None
            try:
                title = i.find_element(
                    By.CSS_SELECTOR, ".search-business-snippet-view__title"
                ).text
            except:
                print("Нет селектора: НаЗвание")
                title = None
            try:
                rating_yandex = i.find_element(
                    By.CSS_SELECTOR, ".business-rating-badge-view__rating-text"
                ).text
            except:
                print("Нет селектора:  Рейтинг")
                rating_yandex = None
            try:
                estimation = i.find_element(
                    By.CSS_SELECTOR, ".business-rating-amount-view"
                ).text
            except:
                print("Наверно нет такого селектора: Оценки")
                estimation = None
            item = {
                "link": link,
                "title": title,
                "rating_yandex": rating_yandex,
                "estimation": estimation,
                "city": self.location,
                "category": self.category,
            }
            print(item)
            from core.db import DB
            db = DB()
            db.add_items_link(item)


    def run(self):

        self.setup_selenium()
        self.create_web_driver()
        time.sleep(5)
        print(self.__parse_block_page())


if __name__ == "__main__":
    print(ParserCard("Бассейны", "Воронеж", 10).run())
