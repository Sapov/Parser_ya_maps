from celery import Celery
from core.config import settings
import logging
import os

logger = logging.getLogger(__name__)

# Создаем экземпляр Celery
celery_app = Celery(
    "worker",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
    include=["tasks"]  # Список модулей с задачами
)

# Настройки Celery
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_time_limit=3600 * 480,  # 20 дней
    task_soft_time_limit=3600 * 480 - 60,  # 20 дней - 60 сек
    worker_prefetch_multiplier=1,
    result_expires=86400,  # Результаты хранятся 24 часа
    task_acks_late=True,  # Подтверждение после выполнения
    worker_max_tasks_per_child=1000,  # Перезапуск воркера после 1000 задач
    task_always_eager=False,  # Должно быть False для асинхронной работы
    task_eager_propagates=False,  # Должно быть False
    task_ignore_result=False,
)

# Настройка логов
celery_app.conf.update(
    worker_log_format="[%(asctime)s: %(levelname)s/%(processName)s] %(message)s",
    worker_task_log_format="[%(asctime)s: %(levelname)s/%(processName)s] %(task_name)s[%(task_id)s]: %(message)s",
)

# ============== ПРАВИЛЬНАЯ НАСТРОЙКА ПУЛА ДЛЯ CELERY ==============

# Глобальная переменная для пула в каждом воркере
_driver_pool = None


def get_driver_pool_for_worker():
    """Получение пула драйверов для текущего воркера"""
    global _driver_pool
    if _driver_pool is None:
        from parser.parser_card import get_driver_pool

        # Важно: каждый воркер создает свой пул
        # Не используйте max_drivers=5 если у вас много воркеров!
        # Например: 4 воркера * 5 драйверов = 20 Chrome процессов
        pool_config = {
            'version_chrome': 146,
            'page_load_timeout': 10
        }

        _driver_pool = get_driver_pool(
            max_drivers=2,  # Уменьшил с 5 до 2 на воркер
            config=pool_config
        )
        logger.info(f"Пул драйверов создан для воркера PID={os.getpid()}")

    return _driver_pool


# ============== СИГНАЛЫ CELERY ДЛЯ FASTAPI ==============

@celery_app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    """
    Этот сигнал срабатывает ПРИ ЗАПУСКЕ Celery beat (планировщика)
    НЕ ИСПОЛЬЗУЙТЕ его для инициализации драйверов!
    """
    logger.info("Celery beat/worker configured (это НЕ инициализация драйверов)")
    # НЕ вызывайте get_driver_pool() здесь!


@celery_app.on_after_finalize.connect
def cleanup(sender, **kwargs):
    """
    Этот сигнал срабатывает при завершении Celery
    НЕ ИСПОЛЬЗУЙТЕ его для очистки драйверов в воркерах!
    """
    logger.info("Celery is shutting down (глобальная очистка)")
    # Очистка будет в worker_process_shutdown


# ============== ПРАВИЛЬНЫЕ СИГНАЛЫ ДЛЯ ВОРКЕРОВ ==============

from celery.signals import worker_process_init, worker_process_shutdown


@worker_process_init.connect
def init_worker(**kwargs):
    """
    ✅ ПРАВИЛЬНО: инициализация пула при старте КАЖДОГО воркер-процесса
    """
    pid = os.getpid()
    logger.info(f"🟢 Инициализация воркер-процесса {pid}")

    # Создаем пул для этого процесса
    pool = get_driver_pool_for_worker()
    logger.info(f"✅ Пул драйверов готов для воркера {pid} (max_drivers=2)")


@worker_process_shutdown.connect
def shutdown_worker(**kwargs):
    """
    ✅ ПРАВИЛЬНО: закрытие пула при завершении воркер-процесса
    """
    global _driver_pool
    pid = os.getpid()
    logger.info(f"🔴 Завершение воркер-процесса {pid}")

    if _driver_pool:
        try:
            _driver_pool.close_all()
            logger.info(f"✅ Пул драйверов закрыт для воркера {pid}")
        except Exception as e:
            logger.error(f"❌ Ошибка при закрытии пула: {e}")
        finally:
            _driver_pool = None


# ============== ВАШИ ЗАДАЧИ ==============

@celery_app.task(name="run_parser", bind=True, max_retries=3)
def run_parser_task(self, category: str, location: str, quantity: int = None):
    """
    Задача для запуска парсера
    """
    from parser.parser_card import runing_parser  # Импорт внутри задачи!

    try:
        logger.info(f"Запуск парсера: {category} - {location}, quantity={quantity}")

        # Драйвер автоматически берется из пула через контекстный менеджер
        result = runing_parser(category, location, quantity)

        logger.info(f"Парсер завершен: обработано {result} элементов")
        return {
            'status': 'success',
            'category': category,
            'location': location,
            'processed': result
        }

    except Exception as exc:
        logger.error(f"Ошибка в парсере: {exc}", exc_info=True)

        # Retry с задержкой
        raise self.retry(
            exc=exc,
            countdown=60 * (2 ** self.request.retries),  # 60, 120, 240 сек
            max_retries=3
        )


# ============== МОНИТОРИНГ (опционально) ==============

@celery_app.task(name="monitor_driver_pool")
def monitor_driver_pool_task():
    """
    Задача для мониторинга состояния пула
    """
    pool = get_driver_pool_for_worker()

    # Добавьте эти методы в DriverPool, если хотите статистику
    stats = {
        'worker_pid': os.getpid(),
        'pool_size': pool._pool.qsize() if hasattr(pool, '_pool') else 0,
        'created_count': pool._created_count if hasattr(pool, '_created_count') else 0,
    }

    logger.info(f"Статистика пула: {stats}")
    return stats


# В вашем celery_app.py добавьте:

@celery_app.task(name="run_parser", bind=True, max_retries=3, autoretry_for=(Exception,), retry_backoff=True)
def run_parser_task(self, category: str, location: str, quantity: int = None):
    """
    Задача для запуска парсера с авто-повтором при ошибках
    """
    from parser.parser_card import runing_parser

    try:
        logger.info(f"Запуск парсера: {category} - {location}, quantity={quantity}")

        result = runing_parser(category, location, quantity)

        logger.info(f"Парсер завершен: обработано {result} элементов")
        return {
            'status': 'success',
            'category': category,
            'location': location,
            'processed': result
        }

    except Exception as exc:
        logger.error(f"Ошибка в парсере: {exc}", exc_info=True)

        # Автоматический повтор при ошибках соединения
        if "connection refused" in str(exc) or "invalid session id" in str(exc):
            raise self.retry(
                exc=exc,
                countdown=30,  # Подождать 30 секунд перед повтором
                max_retries=3
            )
        raise

# ============== ДЛЯ ЗАПУСКА ==============

if __name__ == "__main__":
    # Запуск Celery воркера из командной строки:
    # celery -A celery_app worker --loglevel=info --concurrency=2
    celery_app.start()