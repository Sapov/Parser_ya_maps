import asyncio
import json
import time
import random
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, field
from contextlib import contextmanager
from pathlib import Path
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, WebDriverException

from core.db import DB
from parser.parser_card import ParserCard, ParserConfig
from selenium.webdriver.common.by import By
import logging

from parser.parser_site import ParseSite

logger = logging.getLogger(__name__)


@dataclass
class PageParserConfig:
    """Конфигурация парсера страниц"""
    scroll_delay_min: int = 1
    scroll_delay_max: int = 5
    max_retries: int = 3
    retry_delay: int = 2
    page_load_timeout: int = 10
    save_to_json: bool = True


class PageParser:
    """
    Парсер для извлечения детальной информации со страниц организаций.
    Получает из базы записи по категории и городу, извлекает телефон, адрес, сайт.
    """

    # CSS селекторы для извлечения данных
    SELECTORS = {
        "name": {
            "selector": (By.TAG_NAME, "H1"),
            "attribute": "text",
            "required": False,
            "clean": True
        },
        "phone": {
            "selector": (By.CSS_SELECTOR, ".orgpage-phones-view__phone-number"),
            "attribute": "text",
            "required": False,
            "clean": True
        },
        "address": {
            "selector": (By.CSS_SELECTOR, ".orgpage-header-view__address"),
            "attribute": "text",
            "required": False,
            "clean": True,
            "replace_newlines": True
        },
        "site": {
            "selector": (By.CSS_SELECTOR, ".business-urls-view__text"),
            "attribute": "text",
            "required": False,
            "clean": True
        }
    }

    def __init__(self, category: str, location: str, config: Optional[PageParserConfig] = None):
        self.category = category
        self.location = location
        self.config = config or PageParserConfig()
        self.db = DB()
        self.driver = None
        self.wait = None
        self.processed_items: List[Dict] = []

        # Статистика
        self.stats = {
            "total": 0,
            "successful": 0,
            "failed": 0,
            "no_phone": 0,
            "no_address": 0,
            "no_site": 0,
            "no_name": 0
        }

    def __enter__(self):
        """Контекстный менеджер для автоматического закрытия драйвера"""
        self._setup_driver()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._close_driver()

    @contextmanager
    def _driver_context(self):
        """Контекстный менеджер для работы с драйвером"""
        try:
            if not self.driver:
                self._setup_driver()
            yield self.driver
        except Exception as e:
            logger.error(f"Ошибка в контексте драйвера: {e}")
            self._close_driver()
            raise
        finally:
            self._close_driver()

    def _setup_driver(self):
        """Настройка и создание драйвера"""
        try:
            # Создаем временный парсер для получения настроек
            temp_parser = ParserCard(
                category=self.category,
                location=self.location,
                quantity=None
            )
            temp_parser.setup_driver()
            self.driver = temp_parser.driver
            self.driver.set_page_load_timeout(self.config.page_load_timeout)
            self.wait = WebDriverWait(self.driver, self.config.page_load_timeout)
            logger.info("Драйвер успешно настроен")
        except Exception as e:
            logger.error(f"Ошибка настройки драйвера: {e}")
            raise

    def _close_driver(self):
        """Безопасное закрытие драйвера"""
        if self.driver:
            try:
                self.driver.quit()
                logger.debug("Драйвер закрыт")
            except Exception as e:
                logger.warning(f"Ошибка при закрытии драйвера: {e}")
            finally:
                self.driver = None
                self.wait = None

    def _get_random_delay(self) -> float:
        """Получение случайной задержки"""
        return random.uniform(self.config.scroll_delay_min, self.config.scroll_delay_max)

    def _safe_extract_element(self, by: tuple, attribute: str = "text",
                              replace_newlines: bool = False) -> Optional[str]:
        """Безопасное извлечение данных из элемента с ожиданием"""
        try:
            # Ожидаем появления элемента
            element = self.wait.until(
                EC.presence_of_element_located(by)
            )

            # Извлекаем значение
            if attribute == "text":
                value = element.text
            else:
                value = element.get_attribute(attribute)

            if not value:
                return None

            # Очистка данных
            if replace_newlines and isinstance(value, str):
                value = value.replace("\n", " ").strip()
            elif isinstance(value, str):
                value = value.strip()

            return value if value else None

        except NoSuchElementException:
            logger.debug(f"Элемент не найден: {by}")
            return None
        except Exception as e:
            logger.debug(f"Ошибка извлечения данных {by}: {e}")
            return None

    def _extract_page_data(self, url: str) -> Dict[str, Any]:
        """Извлечение всех данных со страницы организации"""
        data = {}

        for field_name, field_config in self.SELECTORS.items():
            by = field_config["selector"]
            attribute = field_config["attribute"]
            replace_newlines = field_config.get("replace_newlines", False)

            value = self._safe_extract_element(by, attribute, replace_newlines)

            if value:
                data[field_name] = value
            elif field_config.get("required"):
                logger.warning(f"Обязательное поле {field_name} не найдено на {url}")
                data[field_name] = ""
            else:
                data[field_name] = ""

        return data

    def _process_single_url(self, item: Dict, retry_count: int = 0) -> Optional[Dict]:
        """Обработка одного URL с повторными попытками"""
        url = item.get("link")

        if not url:
            logger.warning("Пустой URL, пропускаем")
            return None

        try:
            logger.info(f"[{item.get('id')}] Открываю: {url}")
            self.driver.get(url)

            # Небольшая задержка для загрузки страницы
            time.sleep(self._get_random_delay())

            # Извлекаем данные со страницы
            page_data = self._extract_page_data(url)

            # Объединяем с исходными данными
            result = {**item, **page_data}

            # Обновляем статистику
            self.stats["successful"] += 1
            if not result.get("phone"):
                self.stats["no_phone"] += 1
            if not result.get("address"):
                self.stats["no_address"] += 1
            if not result.get("site"):
                self.stats["no_site"] += 1
            if not result.get("name"):
                self.stats["no_name"] += 1

            logger.info(f"✓ [{item.get('id')}] {item.get('title')} - Телефон: {result.get('phone', 'нет')}")

            return result

        except WebDriverException as e:
            logger.error(f"Ошибка при открытии {url}: {e}")

            if retry_count < self.config.max_retries:
                logger.info(f"Повторная попытка {retry_count + 1}/{self.config.max_retries}")
                time.sleep(self.config.retry_delay)
                self._close_driver()
                self._setup_driver()
                return self._process_single_url(item, retry_count + 1)

            self.stats["failed"] += 1
            return None

        except Exception as e:
            logger.error(f"Неожиданная ошибка при обработке {url}: {e}")
            self.stats["failed"] += 1
            return None

    def get_records_from_db(self) -> List[Dict]:
        """Получение записей из базы данных по категории и городу"""
        try:
            # Получаем записи из БД
            records = self.db.get_by_category_and_city(
                category_name=self.category,
                city_name=self.location
            )

            if not records:
                logger.warning(f"Нет записей для категории '{self.category}' и города '{self.location}'")
                return []

            # Преобразуем в нужный формат
            formatted_records = []
            for record in records:
                formatted_records.append({
                    "id": record.get("id"),
                    "title": record.get("title"),
                    "link": record.get("link"),
                    "rating_yandex": record.get("rating_yandex"),
                    "estimation": record.get("estimation"),
                })

            self.stats["total"] = len(formatted_records)
            logger.info(f"Получено {len(formatted_records)} записей из БД")

            return formatted_records

        except Exception as e:
            logger.error(f"Ошибка получения записей из БД: {e}")
            return []

    def process_all_records(self) -> List[Dict]:
        """Обработка всех записей"""
        records = self.get_records_from_db()

        if not records:
            logger.warning("Нет записей для обработки")
            return []

        results = []

        with self._driver_context():
            for idx, record in enumerate(records, 1):
                try:
                    logger.info(f"\n{'=' * 60}")
                    logger.info(f"Обработка {idx}/{len(records)}")
                    logger.info(f"{'=' * 60}")

                    result = self._process_single_url(record)

                    if result:
                        # Обновляем запись в базе
                        self.db.update_record(result)
                        results.append(result)
                        self.processed_items.append(result)

                    # Задержка между запросами
                    time.sleep(self._get_random_delay())

                except Exception as e:
                    logger.error(f"Критическая ошибка при обработке записи {record.get('id')}: {e}")
                    continue

        return results

    def _log_statistics(self):
        """Вывод статистики"""
        logger.info(f"\n{'=' * 60}")
        logger.info("📊 СТАТИСТИКА ОБРАБОТКИ")
        logger.info(f"{'=' * 60}")
        logger.info(f"📦 Всего записей: {self.stats['total']}")
        logger.info(f"✅ Успешно обработано: {self.stats['successful']}")
        logger.info(f"❌ Ошибок: {self.stats['failed']}")
        logger.info(f"📞 Нет телефона: {self.stats['no_phone']}")
        logger.info(f"📍 Нет адреса: {self.stats['no_address']}")
        logger.info(f"🌐 Нет сайта: {self.stats['no_site']}")
        logger.info(f"🏷 Нет названия: {self.stats['no_name']}")
        logger.info(f"{'=' * 60}\n")

    def save_results_to_json(self):
        """Сохранение результатов в JSON файл"""
        if not self.config.save_to_json or not self.processed_items:
            return

        try:
            # Создаем директорию для результатов, если её нет
            output_dir = Path("parsed_results")
            output_dir.mkdir(exist_ok=True)

            # Формируем имя файла
            filename = output_dir / f"{self.category}_{self.location}_details.json"

            with open(filename, "w", encoding="utf-8") as f:
                json.dump(self.processed_items, f, ensure_ascii=False, indent=4)

            logger.info(f"Результаты сохранены в {filename}")

        except Exception as e:
            logger.error(f"Ошибка сохранения JSON: {e}")

    def run_additional_parsing(self):
        """Запуск дополнительного парсинга (сайтов)"""
        try:
            logger.info("\n🔄 Запуск дополнительного парсинга сайтов")

            # Получаем данные для дополнительного парсинга
            records = self.db.get_by_category_and_city(
                category_name=self.category,
                city_name=self.location
            )

            if records:
                asyncio.run(ParseSite(records).main())
                logger.info("Дополнительный парсинг завершен")
            else:
                logger.warning("Нет данных для дополнительного парсинга")

        except Exception as e:
            logger.error(f"Ошибка дополнительного парсинга: {e}")

    def run(self) -> None:
        """Основной метод запуска парсера"""
        start_time = time.time()

        logger.info(f"\n{'=' * 60}")
        logger.info(f"🚀 ЗАПУСК ПАРСЕРА СТРАНИЦ")
        logger.info(f"📂 Категория: {self.category}")
        logger.info(f"📍 Город: {self.location}")
        logger.info(f"{'=' * 60}\n")

        try:
            # Обрабатываем все записи
            results = self.process_all_records()

            # Выводим статистику
            self._log_statistics()

            # Сохраняем результаты
            self.save_results_to_json()

            # Запускаем дополнительный парсинг
            self.run_additional_parsing()

            # Итоговое время выполнения
            elapsed_time = time.time() - start_time
            logger.info(f"⏱ Общее время выполнения: {elapsed_time:.2f} секунд")

        except Exception as e:
            logger.error(f"Критическая ошибка в процессе парсинга: {e}", exc_info=True)
            raise
        finally:
            self._close_driver()


class AsyncPageParser(PageParser):
    """Асинхронная версия парсера страниц"""

    async def run_async(self) -> None:
        """Асинхронный запуск парсера"""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.run)


if __name__ == "__main__":
    # Настройка логирования
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Создание конфигурации
    config = PageParserConfig(
        scroll_delay_min=2,
        scroll_delay_max=4,
        max_retries=3,
        save_to_json=True
    )

    # Запуск парсера
    parser = PageParser(
        category='Агентство недвижимости',
        location='Саратов'
    )

    try:
        parser.run()


    except KeyboardInterrupt:
        logger.info("Парсинг прерван пользователем")
    except Exception as e:
        logger.error(f"Ошибка: {e}")