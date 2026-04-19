import queue
import threading
import time
from typing import Optional, Dict
from contextlib import contextmanager
import undetected_chromedriver as uc
import logging

logger = logging.getLogger(__name__)


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
                        # Пытаемся извлечь из очереди (если там есть)
                        try:
                            self._pool.get_nowait()
                        except queue.Empty:
                            pass

                        self._quit_driver(driver)
                        del self._last_used[driver]
                        logger.info(f"Драйвер удален из-за бездействия (idle: {self.driver_idle_timeout} сек)")
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

        # Применяем пользовательские настройки из конфига
        if self.config.get('options'):
            for opt in self.config['options']:
                options.add_argument(opt)

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
            # Пытаемся получить драйвер из очереди (блокируем на 5 секунд)
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
            if driver and driver.service.process and driver.service.process.poll() is None:
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