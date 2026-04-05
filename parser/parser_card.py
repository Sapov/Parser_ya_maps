import random
import time
from dataclasses import dataclass
from typing import Optional, Dict

from selenium.common import NoSuchElementException
from selenium.webdriver.support.wait import WebDriverWait

from core.db import DB
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By

import logging

logger = logging.getLogger(__name__)


@dataclass
class ParserConfig:
    """Конфигурация парсера"""
    url: str = "https://yandex.ru/maps/193/voronezh/search/"
    version_chrome: int = 146
    scroll_delay_min: int = 1
    scroll_delay_max: int = 7
    page_load_timeout: int = 10
    element_wait_timeout: int = 10


class ParserCard:
    # Константы для селекторов
    SELECTORS = {
        "link": (".search-snippet-view .link-overlay", "href", True),  # обязательное поле
        "title": (".search-business-snippet-view__title", "text", False),
        "rating_yandex": (".business-rating-badge-view__rating-text", "text", False),
        "estimation": (".business-rating-amount-view", "text", False),
    }

    def __init__(self, category: str, location: str, quantity: int, config: ParserConfig = None):
        self.quantity = quantity
        self.location = location
        self.category = category
        self.driver = None
        self.config = config or ParserConfig()

    def __enter__(self):
        """Контекстный менеджер для автоматического закрытия драйвера"""
        self.setup_driver()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def setup_driver(self):
        """Настройка и создание драйвера"""
        options = uc.ChromeOptions()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")

        self.driver = uc.Chrome(
            version_main=self.config.version_chrome,
            options=options
        )
        self.driver.set_page_load_timeout(self.config.page_load_timeout)
        self.wait = WebDriverWait(self.driver, self.config.element_wait_timeout)

    def close(self):
        """Закрытие драйвера"""
        if self.driver:
            self.driver.quit()

    def __create_full_url(self):
        return f"{self.config.url}{self.category} {self.location}"

    def parse(self):
        self.driver.get(self.__create_full_url())
        time.sleep(4)

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
        """
        Парсит карточки организаций и сохраняет в БД
        """
        logger.info(f"Начинаем парсинг {len(elements)} карточек")
        db = DB()

        parsed_count = 0
        error_count = 0

        for idx, element in enumerate(elements, 1):
            try:
                item = self._parse_single_card_safe(element)
                if item:
                    db.add_items_link(item)
                    parsed_count += 1
                    logger.debug(f"Успешно распарсена карточка {idx}: {item.get('title', 'без названия')}")
                else:
                    error_count += 1
                    logger.warning(f"Не удалось распарсить карточку {idx}")

            except Exception as e:
                error_count += 1
                logger.error(f"Критическая ошибка при парсинге карточки {idx}: {e}", exc_info=True)

        logger.info(f"Парсинг завершен: успешно {parsed_count}, ошибок {error_count}")

    def _parse_single_card_safe(self, element) -> Optional[Dict]:
        """
        Безопасный парсинг одной карточки с обработкой всех ошибок
        """
        item = {
            "city": self.location,
            "category": self.category,
        }

        has_link = False

        for field_name, (selector, attr_type, is_required) in self.SELECTORS.items():
            value = self._safe_extract(element, selector, attr_type)

            if is_required and value is None:
                logging.error(f"Обязательное поле {field_name} не найдено, карточка пропущена")
                return None

            item[field_name] = value

            if field_name == "link" and value:
                has_link = True

        # Дополнительная валидация
        if not has_link:
            logging.error("Карточка не содержит ссылки, пропускаем")
            return None

        # Очистка данных
        item = self._clean_item_data(item)

        return item

    def _safe_extract(self, element, selector: str, attr_type: str) -> Optional[str]:
        """
        Безопасное извлечение данных из элемента
        """
        try:
            found_element = element.find_element(By.CSS_SELECTOR, selector)

            if attr_type == "text":
                return found_element.text.strip() if found_element.text else None
            elif attr_type == "href":
                return found_element.get_attribute("href")
            else:
                return found_element.get_attribute(attr_type)

        except NoSuchElementException:
            logging.debug(f"Селектор не найден: {selector}")
            return None
        except Exception as e:
            logging.debug(f"Ошибка при извлечении {selector}: {e}")
            return None

    def _clean_item_data(self, item: Dict) -> Dict:
        """
        Очистка и нормализация данных
        """
        # Очистка рейтинга (замена запятой на точку)
        if item.get("rating_yandex"):
            item["rating_yandex"] = item["rating_yandex"].replace(",", ".")

        # Очистка оценки (удаление лишних символов)
        if item.get("estimation"):
            # Извлекаем только цифры из строки типа "123 оценки"
            import re
            numbers = re.findall(r'\d+', item["estimation"])
            if numbers:
                item["estimation"] = int(numbers[0])

        return item


    def _get_random_delay(self) -> float:
        """Получение случайной задержки"""
        return random.uniform(self.config.scroll_delay_min, self.config.scroll_delay_max)

    def run(self):
        self.setup_driver()
        self.parse()




if __name__ == "__main__":
    print(ParserCard("Агенство недвижимости", "Тамбов", 10).run())
