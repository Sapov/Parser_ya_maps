import atexit
import random
import shutil
import tempfile
import time
import re
from dataclasses import dataclass
from typing import Optional, Dict, List
from functools import lru_cache
from core.clianing_tmp import clean_tmp
from selenium.common import NoSuchElementException
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.remote.webelement import WebElement

from core.db import DB
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By

import logging

from parser.parser_ya_page import PageParser

logger = logging.getLogger(__name__)


@dataclass
class ParserConfig:
    """Конфигурация парсера"""
    url: str = "https://yandex.ru/maps/193/voronezh/search/"
    version_chrome: int = 146
    scroll_delay_min: int = 2
    scroll_delay_max: int = 8
    page_load_timeout: int = 30
    element_wait_timeout: int = 20
    max_scroll_attempts: int = 150
    batch_size: int = 10


# ============== ПАРСЕР ==============

class ParserCard:
    """
    Парсер карточек Я.Карт
    """

    # Константы для селекторов
    SELECTORS = {
        "link": (".search-snippet-view .link-overlay", "href", True),
        "title": (".search-business-snippet-view__title", "text", False),
        "rating_yandex": (".business-rating-badge-view__rating-text", "text", False),
        "estimation": (".business-rating-amount-view", "text", False),
    }

    # Компилируем регулярные выражения один раз
    _RATING_CLEANER = re.compile(r'[^\d,.]')
    _NUMBERS_EXTRACTOR = re.compile(r'\d+')

    def __init__(self, category: str, location: str, quantity: int = None, config: ParserConfig = None):
        self.temp_dir = None
        self.quantity = quantity
        self.location = location
        self.category = category
        self.config = config or ParserConfig()
        self.driver = None
        self.wait = None
        self._processed_count = 0
        self._db = None

    @property
    def db(self):
        """Ленивая инициализация БД"""
        if self._db is None:
            self._db = DB()
        return self._db

        def __enter__(self):
            """Контекстный менеджер для автоматического закрытия драйвера"""

    def setup_driver(self):




        options = uc.ChromeOptions()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")

        # Создаём свою временную папку
        self.temp_dir = tempfile.mkdtemp()

        options.add_argument(f'--user-data-dir={self.temp_dir}')
        options.add_argument('--disk-cache-dir=/tmp/cache')

        # Регистрируем очистку
        atexit.register(self.close)

        # Дополнительные опции для стабильности
        # options.add_argument("--disable-features=VizDisplayCompositor")
        # options.add_argument("--disable-logging")
        # options.add_argument("--log-level=3")

        self.driver = uc.Chrome(
            version_main=self.config.version_chrome,
            options=options
        )
        self.driver.set_page_load_timeout(self.config.page_load_timeout)
        self.wait = WebDriverWait(self.driver, self.config.element_wait_timeout)
        logger.info(f"Драйвер настроен для {self.category} - {self.location}")

    def close(self):
        """Закрытие драйвера"""
        if self.driver:
            try:
                self.driver.quit()
                logger.info("Драйвер закрыт")

            except Exception as e:
                logger.warning(f"Ошибка при закрытии драйвера: {e}")
            finally:
                self.driver = None
                self.wait = None
                # Удаляем временную папку
                shutil.rmtree(self.temp_dir, ignore_errors=True)
                clean_tmp()




    @lru_cache(maxsize=128)
    def _get_full_url(self) -> str:
        """Формирует полный URL с кэшированием"""
        return f"{self.config.url}{self.category} {self.location}"

    def _get_random_delay(self) -> float:
        """Получение случайной задержки"""
        return random.uniform(self.config.scroll_delay_min, self.config.scroll_delay_max)

    def _scroll_to_element(self, element: WebElement):
        """Плавная прокрутка к элементу"""
        self.driver.execute_script(
            "arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});",
            element
        )
        time.sleep(0.5)

    def _wait_for_elements(self, selector: str, timeout: int = None) -> List[WebElement]:
        """Ожидание появления элементов с обработкой ошибок"""
        timeout = timeout or self.config.element_wait_timeout
        try:
            WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, selector))
            )
            return self.driver.find_elements(By.CSS_SELECTOR, selector)
        except Exception as e:
            logger.warning(f"Элементы не найдены по селектору {selector}: {e}")
            return []

    def _safe_extract(self, element: WebElement, selector: str, attr_type: str) -> Optional[str]:
        """Безопасное извлечение данных из элемента"""
        try:
            found_element = element.find_element(By.CSS_SELECTOR, selector)

            if attr_type == "text":
                value = found_element.text
                return value.strip() if value else None
            elif attr_type == "href":
                return found_element.get_attribute("href")
            else:
                return found_element.get_attribute(attr_type)

        except NoSuchElementException:
            logger.debug(f"Селектор не найден: {selector}")
            return None
        except Exception as e:
            logger.debug(f"Ошибка при извлечении {selector}: {e}")
            return None

    def _clean_item_data(self, item: Dict) -> Dict:
        """Очистка и нормализация данных"""
        if rating := item.get("rating_yandex"):
            cleaned = self._RATING_CLEANER.sub('', rating)
            item["rating_yandex"] = cleaned.replace(",", ".")

        if estimation := item.get("estimation"):
            numbers = self._NUMBERS_EXTRACTOR.findall(estimation)
            if numbers:
                item["estimation"] = int(numbers[0])

        return item

    def _parse_single_card(self, element: WebElement) -> Optional[Dict]:
        """Парсинг одной карточки"""
        item = {
            "city": self.location,
            "category": self.category,
        }

        has_required = False

        for field_name, (selector, attr_type, is_required) in self.SELECTORS.items():
            value = self._safe_extract(element, selector, attr_type)

            if is_required:
                if value:
                    has_required = True
                else:
                    logger.debug(f"Пропущена карточка: нет обязательного поля {field_name}")
                    return None

            item[field_name] = value

        if not has_required:
            logger.debug("Карточка не содержит обязательных полей, пропускаем")
            return None

        return self._clean_item_data(item)

    def _process_batch(self, elements: List[WebElement]) -> int:
        """Пакетная обработка карточек"""
        parsed_items = []

        for element in elements:
            if self.quantity and self._processed_count >= self.quantity:
                break

            item = self._parse_single_card(element)
            if item:
                parsed_items.append(item)
                self._processed_count += 1

        if parsed_items:
            self._save_items_batch(parsed_items)

        return len(parsed_items)

    def _save_items_batch(self, items: List[Dict]):
        """Массовое сохранение элементов в БД"""
        try:
            if hasattr(self.db, 'add_items_batch'):
                self.db.add_items_batch(items)
            else:
                for item in items:
                    self.db.add_items_link(item)
            logger.info(f"Сохранено {len(items)} элементов в БД")
        except Exception as e:
            logger.error(f"Ошибка сохранения в БД: {e}")

    def _scroll_and_collect(self) -> List[WebElement]:
        """Прокрутка страницы и сбор элементов"""
        all_elements = []
        scroll_attempts = 0
        no_new_elements_count = 0

        logger.info(f"Начинаем сбор элементов для категории: {self.category}, город: {self.location}")

        while True:
            if self.quantity and len(all_elements) >= self.quantity:
                logger.info(f"Достигнут лимит в {self.quantity} элементов")
                break

            if scroll_attempts >= self.config.max_scroll_attempts:
                logger.warning(f"Достигнуто максимальное количество прокруток: {self.config.max_scroll_attempts}")
                break

            current_elements = self._wait_for_elements(".search-snippet-view")

            if not current_elements:
                logger.warning("Элементы не найдены")
                break

            current_count = len(current_elements)
            old_count = len(all_elements)

            all_elements = current_elements

            logger.info(
                f"Найдено элементов: {current_count} из города {self.location} (новых: {current_count - old_count})")

            if current_count == old_count:
                no_new_elements_count += 1
                if no_new_elements_count >= 3:
                    logger.info("Новых элементов не добавлено, завершаем сбор")
                    break
            else:
                no_new_elements_count = 0

            if current_elements:
                try:
                    self._scroll_to_element(current_elements[-1])
                    time.sleep(self._get_random_delay())
                except Exception as e:
                    logger.warning(f"Ошибка при прокрутке: {e}")
                    break

            scroll_attempts += 1

        if self.quantity and len(all_elements) > self.quantity:
            all_elements = all_elements[:self.quantity]

        logger.info(f"Собрано {len(all_elements)} элементов для обработки")
        return all_elements

    def parse(self) -> int:
        """Основной метод парсинга"""
        start_time = time.time()
        self.setup_driver()
        try:
            logger.info(f"Загрузка страницы: {self._get_full_url()}")
            self.driver.get(self._get_full_url())
            time.sleep(2)

            elements = self._scroll_and_collect()

            if not elements:
                logger.warning("Не найдено элементов для парсинга")
                return 0

            processed_count = self._process_batch(elements)

            elapsed_time = time.time() - start_time
            logger.info(f"\n{'=' * 60}")
            logger.info(f"📊 СТАТИСТИКА ПАРСИНГА")
            logger.info(f"{'=' * 60}")
            logger.info(f"⏱ Время выполнения: {elapsed_time:.2f} секунд")
            logger.info(f"📦 Собрано элементов: {len(elements)}")
            logger.info(f"✅ Обработано и сохранено: {processed_count}")
            logger.info(f"⚡ Скорость: {processed_count / elapsed_time:.2f} элементов/сек")
            logger.info(f"{'=' * 60}\n")
            self.close()
            return processed_count

        except Exception as e:
            logger.error(f"Критическая ошибка при парсинге: {e}", exc_info=True)
            raise



# ============== ФУНКЦИЯ ЗАПУСКА ==============

def runing_parser(category: str, location: str, quantity: int = None, config: ParserConfig = None) -> int:
    """
    Удобная функция для запуска парсера
    """
    parser = ParserCard(category, location, quantity, config)
    result = parser.parse()

    PageParser(category, location).run()
    return result


if __name__ == "__main__":
    # Настройка логирования
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Запуск парсера
    result = runing_parser("Рисование", "Москва", 20)

    print(f"Парсинг завершен. Обработано элементов: {result}")
