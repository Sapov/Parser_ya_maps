from celery import Celery
from core.config import settings

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
    task_time_limit=3600 * 3,  # 2 часа
    task_soft_time_limit=3600 * 3 - 60,  # 2 часа - 60 сек
    worker_prefetch_multiplier=1,
    result_expires=86400,  # Результаты хранятся 24 часа
    task_acks_late=True,  # Подтверждение после выполнения
    worker_max_tasks_per_child=1000,  # Перезапуск воркера после 1000 задач
)

# Настройка логов
celery_app.conf.update(
    worker_log_format="[%(asctime)s: %(levelname)s/%(processName)s] %(message)s",
    worker_task_log_format="[%(asctime)s: %(levelname)s/%(processName)s] %(task_name)s[%(task_id)s]: %(message)s",
)

if __name__ == "__main__":
    celery_app.start()