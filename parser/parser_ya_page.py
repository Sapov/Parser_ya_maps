import json
import math
import time
import random

from parser.parser_card import ParserCard
from core.db import DB
from selenium.webdriver.common.by import By
import logging

logger = logging.getLogger(__name__)

"""
Логика парсер достает из базы окно в 10 записей, обрабаытвает 
закрывает браузер, потом достает в цикле новые 50 записей"""


class ParserPage(ParserCard):
    """Что умеет этот класс:
    1. читать из базы по 10 записей
    2. Получаем со страницы (телефон адресс сайт)
    """

    def __init__(self):
        # super().__init__()
        self.list_elements = []
        self.link_list_items = []

    def get_data_set(self, number_of_entries: int):
        # читаем из базы по страницам в 10 записей
        # number_of_entries = 10
        db = DB()
        links = db.get_all_links()
        count = len(links)
        logger.info(f"Всего {count} записей в базе")
        count = math.ceil(count / number_of_entries)
        logger.info(f"Это {count} Страниц по {number_of_entries} записей")
        for j in range(1, count + 1):
            set_items = db.get_links_paginated(j, number_of_entries)
            logging.info(f"[INFO] Проходим набор №  {j}")

            for i in set_items:
                self.link_list_items.append(
                    {
                        "id": i.id,
                        "title": i.title,
                        "link": i.link,
                        "rating_yandex": i.rating_yandex,
                        "estimation": i.estimation,
                    }
                )
            self.__open_page()
            self.link_list_items.clear()

    def __open_page(self):
        self.setup_selenium()
        self.create_web_driver()
        """Получаем со страницы организации телефон, адрес, сайт"""
        for index, val in enumerate(self.link_list_items):
            time.sleep(random.randint(1, 5))
            items = {} | val
            try:
                logging.info(f'Open Link {val["link"]}')
                self.driver.get(val["link"])
                try:
                    items.setdefault(
                        "name", self.driver.find_element(By.TAG_NAME, "H1").text
                    )
                except:
                    print("NO NAME")
                    items["name"] = ""
                try:
                    items.setdefault(
                        "phone",
                        self.driver.find_element(
                            By.CSS_SELECTOR, ".orgpage-phones-view__phone-number"
                        ).text,
                    )
                except:
                    print("Нет телефона")
                    items["phone"] = ""
                try:
                    items.setdefault(
                        "address",
                        self.driver.find_element(
                            By.CSS_SELECTOR, ".orgpage-header-view__address"
                        ).text.replace("\n", " "),
                    )
                except:
                    print("NO Address")
                    items["address"] = ""
                try:
                    items.setdefault(
                        "site",
                        self.driver.find_element(
                            By.CSS_SELECTOR, ".business-urls-view__text"
                        ).text,
                    )
                except:
                    items["site"] = ""
            except:
                logging.warning(f"Сcылка не открылась")

            logger.info(f"NOMBER {index} {items} \n ")
            print("*" * 50)
            # Обновляем базу
            db = DB()
            db.update_record(items)

            self.list_elements.append(items)
        self.save_data(self.list_elements)

        self.driver.close()
        self.driver.quit()



    def save_data(self, new_list: list):
        with open(f"mail.json", "w", encoding="utf-8") as file:
            json.dump(new_list, file, ensure_ascii=False, indent=4)

    def run(self) -> None:
        self.get_data_set(10)


if __name__ == "__main__":
    a = ParserPage()
    a.run()