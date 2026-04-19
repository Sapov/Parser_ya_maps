import random
import time
import re
import queue
import threading
from dataclasses import dataclass
from typing import Optional, Dict, List, Any
from contextlib import contextmanager
from functools import lru_cache

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


# ============== ПУЛ ДРАЙВЕРОВ ==============

class DriverPool:
    """Пул undetected-chromedriver драйверов для переиспользования"""

    def __init__(self, max_drivers: int = 3, driver_idle_timeout: int = 300, config: Dict = None):
        """
        Args:
            max_drivers: Максимальное количество драйверов в пуле
            driver_idle_timeout: Таймаут бездействия драйвера в секундах
            config: Конфигурация для создания драйверов
        """
        self.max_drivers = max_drivers
        self.driver_idle_timeout = driver_idle_timeout
        self.config = config or {}

        # Очередь свободных драйверов
        self._pool = queue.Queue(maxsize=max_drivers)

        # Словарь для отслеживания времени последнего использования
        self._last_used: Dict[uc.Chrome, float] = {}

        # Флаг работы монитора
        self._monitor_running = False
        self._monitor_thread = None

        # Счетчик созданных драйверов
        self._created_count = 0

        # Запускаем монитор для очистки "старых" драйверов
        self._start_monitor()

    def _start_monitor(self):
        """Запускает фоновый поток для очистки бездействующих драйверов"""
        self._monitor_running = True
        self._monitor_thread = threading.Thread(target=self._cleanup_idle_drivers, daemon=True)
        self._monitor_thread.start()

    def _cleanup_idle_drivers(self):
        """Фоновый поток для удаления драйверов, которые долго не использовались"""
        while self._monitor_running:
            time.sleep(60)  # Проверяем раз в минуту

            try:
                # Создаем временный список для проверки
                drivers_to_remove = []

                # Проверяем все активные драйверы
                for driver, last_used in list(self._last_used.items()):
                    if time.time() - last_used > self.driver_idle_timeout:
                        drivers_to_remove.append(driver)

                # Удаляем "старые" драйверы
                for driver in drivers_to_remove:
                    try:
                        self._quit_driver(driver)
                        del self._last_used[driver]
                        logger.info(f"Драйвер удален из-за бездействия")
                    except Exception as e:
                        logger.warning(f"Ошибка при очистке драйвера: {e}")

            except Exception as e:
                logger.error(f"Ошибка в мониторе очистки: {e}")

    def _create_driver(self) -> uc.Chrome:
        """Создает новый драйвер с конфигурацией"""
        options = uc.ChromeOptions()

        # Применяем стандартные настройки
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-features=VizDisplayCompositor")
        options.add_argument("--disable-logging")
        options.add_argument("--log-level=3")

        driver = uc.Chrome(
            version_main=self.config.get('version_chrome', 146),
            options=options
        )

        driver.set_page_load_timeout(self.config.get('page_load_timeout', 10))

        self._created_count += 1
        logger.info(f"Создан новый драйвер (всего создано: {self._created_count})")

        return driver

    def _quit_driver(self, driver: uc.Chrome):
        """Безопасное закрытие драйвера"""
        if driver:
            try:
                driver.quit()
                logger.debug("Драйвер успешно закрыт")
            except Exception as e:
                logger.warning(f"Ошибка при закрытии драйвера: {e}")

    @contextmanager
    def get_driver(self):
        """Получает драйвер из пула (контекстный менеджер)"""
        driver = None

        try:
            # Пытаемся получить драйвер из очереди
            try:
                driver = self._pool.get_nowait()
                logger.debug("Используем существующий драйвер из пула")
            except queue.Empty:
                # Если нет свободных драйверов, создаем новый (если не превышен лимит)
                if self._created_count < self.max_drivers:
                    driver = self._create_driver()
                else:
                    # Ждем освобождения драйвера
                    logger.debug("Все драйверы заняты, ожидаем...")
                    driver = self._pool.get(timeout=30)

            # Обновляем время последнего использования
            self._last_used[driver] = time.time()

            yield driver

        except Exception as e:
            logger.error(f"Ошибка при получении драйвера: {e}")
            if driver:
                self._quit_driver(driver)
            raise
        finally:
            # Возвращаем драйвер в пул или закрываем при ошибке
            if driver and driver.service and driver.service.process and driver.service.process.poll() is None:
                try:
                    # Очищаем куки и кэш перед возвратом
                    driver.delete_all_cookies()
                    driver.execute_script("window.localStorage.clear();")
                    driver.execute_script("window.sessionStorage.clear();")

                    self._pool.put(driver)
                    logger.debug("Драйвер возвращен в пул")
                except Exception as e:
                    logger.warning(f"Ошибка при возврате драйвера в пул: {e}")
                    self._quit_driver(driver)
            elif driver:
                self._quit_driver(driver)

    def close_all(self):
        """Закрывает все драйверы в пуле"""
        self._monitor_running = False

        if self._monitor_thread:
            self._monitor_thread.join(timeout=2)

        # Закрываем все драйверы в очереди
        while not self._pool.empty():
            try:
                driver = self._pool.get_nowait()
                self._quit_driver(driver)
            except queue.Empty:
                break

        self._last_used.clear()
        logger.info("Все драйверы в пуле закрыты")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_all()

        def _is_driver_alive(self, driver: uc.Chrome) -> bool:
            """
            Проверяет, жив ли драйвер и его WebDriver сессия
            """
            if not driver or not driver.service or not driver.service.process:
                return False

            # Проверяем, жив ли процесс
            if driver.service.process.poll() is not None:
                logger.warning(f"Driver process {driver.service.process.pid} is dead")
                return False

            # Проверяем, отвечает ли WebDriver на простой запрос
            try:
                # Пробуем получить текущий URL (быстрая проверка)
                current_url = driver.current_url
                # Пробуем выполнить простой JavaScript
                driver.execute_script("return 1;")
                return True
            except Exception as e:
                logger.warning(f"Driver health check failed: {e}")
                return False

        def _recreate_dead_driver(self, old_driver=None) -> uc.Chrome:
            """
            Создает новый драйвер, закрывая старый если нужно
            """
            if old_driver:
                try:
                    old_driver.quit()
                except:
                    pass

            logger.info("Создаем новый драйвер взамен умершего")
            return self._create_driver()

        @contextmanager
        def get_driver(self):
            """Получает драйвер из пула с проверкой здоровья"""
            driver = None

            try:
                # Пытаемся получить драйвер из очереди
                try:
                    driver = self._pool.get_nowait()

                    # Проверяем, жив ли драйвер
                    if not self._is_driver_alive(driver):
                        logger.warning("Драйвер из пула не отвечает, создаем новый")
                        driver = self._recreate_dead_driver(driver)

                except queue.Empty:
                    # Если нет свободных драйверов, создаем новый
                    if self._created_count < self.max_drivers:
                        driver = self._create_driver()
                    else:
                        # Ждем освобождения драйвера
                        logger.debug("Все драйверы заняты, ожидаем...")
                        driver = self._pool.get(timeout=30)

                        # Проверяем полученный драйвер
                        if not self._is_driver_alive(driver):
                            logger.warning("Драйвер из очереди не отвечает, создаем новый")
                            driver = self._recreate_dead_driver(driver)

                # Обновляем время последнего использования
                self._last_used[driver] = time.time()

                yield driver

            except Exception as e:
                logger.error(f"Ошибка при получении драйвера: {e}")
                if driver:
                    self._quit_driver(driver)
                raise
            finally:
                # Возвращаем драйвер в пул только если он жив
                if driver and self._is_driver_alive(driver):
                    try:
                        # Очищаем куки и кэш перед возвратом
                        driver.delete_all_cookies()
                        driver.execute_script("window.localStorage.clear();")
                        driver.execute_script("window.sessionStorage.clear();")

                        # Переходим на about:blank чтобы освободить память
                        try:
                            driver.get("about:blank")
                        except:
                            pass

                        self._pool.put(driver)
                        logger.debug("Драйвер возвращен в пул")
                    except Exception as e:
                        logger.warning(f"Ошибка при возврате драйвера в пул: {e}")
                        self._quit_driver(driver)
                elif driver:
                    logger.warning("Драйвер мертв, закрываем без возврата в пул")
                    self._quit_driver(driver)



# Глобальный экземпляр пула (создается один раз для всего приложения)
_driver_pool_instance = None


def get_driver_pool(max_drivers: int = 3, config: Dict = None):
    """
    Получение глобального экземпляра пула драйверов (синглтон)
    ВЫЗЫВАТЬ ТОЛЬКО ЭТУ ФУНКЦИЮ для получения пула
    """
    global _driver_pool_instance
    if _driver_pool_instance is None:
        _driver_pool_instance = DriverPool(max_drivers=max_drivers, config=config)
    return _driver_pool_instance


# ============== КОНФИГУРАЦИЯ ==============

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
    Парсер карточек Я.Карт (с использованием пула драйверов)
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
        self.quantity = quantity
        self.location = location
        self.category = category
        self.config = config or ParserConfig()
        self.driver = None
        self.wait = None
        self._processed_count = 0
        self._db = None
        self._driver_context = None  # Будет хранить контекст драйвера

    @property
    def db(self):
        """Ленивая инициализация БД"""
        if self._db is None:
            self._db = DB()
        return self._db

    def __enter__(self):
        """Контекстный менеджер - получаем драйвер из пула"""
        # Получаем глобальный пул (ЭТО ТО МЕСТО, ГДЕ ВЫЗЫВАЕТСЯ get_driver_pool)
        pool_config = {
            'version_chrome': self.config.version_chrome,
            'page_load_timeout': self.config.page_load_timeout
        }

        # ВОТ ЗДЕСЬ вызывается get_driver_pool
        driver_pool = get_driver_pool(max_drivers=3, config=pool_config)

        # Получаем драйвер из пула
        self._driver_context = driver_pool.get_driver()
        self.driver = self._driver_context.__enter__()
        self.wait = WebDriverWait(self.driver, self.config.element_wait_timeout)

        logger.info(f"Драйвер получен из пула для {self.category} - {self.location}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Возвращаем драйвер в пул"""
        if self._driver_context:
            self._driver_context.__exit__(exc_type, exc_val, exc_tb)
            logger.info("Драйвер возвращен в пул")

    @lru_cache(maxsize=128)
    def _get_full_url(self) -> str:
        """Формирует полный URL с кэшированием"""
        return f"{self.config.url}{self.category} {self.location}"

    @contextmanager
    def _wait_for_page_load(self, timeout: int = 10):
        """Контекстный менеджер для ожидания загрузки страницы"""
        old_timeout = self.driver.execute_script("return document.readyState")
        yield
        WebDriverWait(self.driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )

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

            logger.info(f"Найдено элементов: {current_count} из города {self.location} (новых: {current_count - old_count})")

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

            return processed_count

        except Exception as e:
            logger.error(f"Критическая ошибка при парсинге: {e}", exc_info=True)
            raise

    def run(self) -> int:
        """Запуск парсинга (упрощенный интерфейс)"""
        with self:
            return self.parse()

    def _safe_find_elements(self, selector: str, max_retries: int = 3) -> List[WebElement]:
        """
        Безопасный поиск элементов с автоматическим восстановлением драйвера
        """
        for attempt in range(max_retries):
            try:
                return self._wait_for_elements(selector)
            except Exception as e:
                if "invalid session id" in str(e) or "connection refused" in str(e):
                    logger.warning(f"Session lost, attempt {attempt + 1}/{max_retries}")
                    time.sleep(2)
                    # Сигнализируем, что драйвер умер - он будет пересоздан при следующем получении
                    if attempt == max_retries - 1:
                        raise
                else:
                    raise
        return []

    def _scroll_and_collect(self) -> List[WebElement]:
        """
        Прокрутка страницы и сбор элементов с обработкой ошибок
        """
        all_elements = []
        scroll_attempts = 0
        no_new_elements_count = 0

        logger.info(f"Начинаем сбор элементов для категории: {self.category}, город: {self.location}")

        while True:
            try:
                # Проверяем лимит
                if self.quantity and len(all_elements) >= self.quantity:
                    logger.info(f"Достигнут лимит в {self.quantity} элементов")
                    break

                # Проверяем максимальное количество прокруток
                if scroll_attempts >= self.config.max_scroll_attempts:
                    logger.warning(
                        f"Достигнуто максимальное количество прокруток: {self.config.max_scroll_attempts}")
                    break

                # Получаем текущие элементы с защитой
                current_elements = self._safe_find_elements(".search-snippet-view")

                if not current_elements:
                    logger.warning("Элементы не найдены")
                    break

                current_count = len(current_elements)
                old_count = len(all_elements)

                all_elements = current_elements

                logger.info(f"Найдено элементов: {current_count} (новых: {current_count - old_count})")

                # Проверяем, появились ли новые элементы
                if current_count == old_count:
                    no_new_elements_count += 1
                    if no_new_elements_count >= 3:
                        logger.info("Новых элементов не добавлено, завершаем сбор")
                        break
                else:
                    no_new_elements_count = 0

                # Прокручиваем к последнему элементу
                if current_elements:
                    try:
                        self._scroll_to_element(current_elements[-1])
                        time.sleep(self._get_random_delay())
                    except Exception as e:
                        if "invalid session id" in str(e) or "connection refused" in str(e):
                            logger.warning("Сессия потеряна при прокрутке")
                            raise  # Перевыбрасываем для внешней обработки
                        logger.warning(f"Ошибка при прокрутке: {e}")
                        break

                scroll_attempts += 1

            except Exception as e:
                if "invalid session id" in str(e) or "connection refused" in str(e):
                    logger.error(f"Потеряна связь с драйвером: {e}")
                    # Пробуем пересоздать сессию
                    if hasattr(self, '_driver_context'):
                        self._driver_context.__exit__(type(e), e, e.__traceback__)
                        # Получаем новый драйвер
                        self._driver_context = self._driver_pool.get_driver()
                        self.driver = self._driver_context.__enter__()
                        self.wait = WebDriverWait(self.driver, self.config.element_wait_timeout)
                        logger.info("Драйвер пересоздан, продолжаем...")
                        continue
                else:
                    logger.error(f"Критическая ошибка при сборе: {e}")
                    break

        # Обрезаем до нужного количества
        if self.quantity and len(all_elements) > self.quantity:
            all_elements = all_elements[:self.quantity]

        logger.info(f"Собрано {len(all_elements)} элементов для обработки")
        return all_elements



# ============== ФУНКЦИЯ ЗАПУСКА ==============

def runing_parser(category: str, location: str, quantity: int = None, config: ParserConfig = None) -> int:
    """
    Удобная функция для запуска парсера
    """
    with ParserCard(category, location, quantity, config) as parser:
        result = parser.parse()

    PageParser(category, location).run()
    return result


def cleanup_driver_pool():
    """Очистка пула драйверов при завершении"""
    pool = get_driver_pool()
    pool.close_all()

# Регистрируем очистку при завершении
import atexit

atexit.register(cleanup_driver_pool)

# ============== ТОЧКА ВХОДА ==============

if __name__ == "__main__":
    # Настройка логирования
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Запуск парсера
    result = runing_parser("Агентство недвижимости", "Москва", 20)

    print(f"Парсинг завершен. Обработано элементов: {result}")