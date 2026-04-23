import time
from typing import Dict, Any, List
from celery_app import celery_app
import logging

from run_parser import run_parser, parse_all_city

logger = logging.getLogger(__name__)


@celery_app.task(name="tasks.parse", bind=True)
def parse(self, category: str, city: str, quantity: int) -> Dict[str, Any]:
    """
    Запуск парсера по ya картам
    """
    run_parser(category, city, quantity)

    logger.info(f"Парсинг {category} запущен для города {city}")
    return {"message": f"Парсинг {category} запущен для города {city} {self.request.id}"}


@celery_app.task(name="tasks.parser_all_city", bind=True)
def parser_all_city(self, category:str):
    parse_all_city(category)

    logger.info(f"Парсинг {category} запущен по всем городам")
    return {"message": f"Парсинг {category} запущен по всем городам {self.request.id}"}



@celery_app.task(name="tasks.process_data", bind=True)
def process_data(self, data: List[Dict]) -> Dict[str, Any]:
    """
    Обработка данных с прогрессом
    """
    total = len(data)
    processed = 0

    for i, item in enumerate(data):
        # Имитация обработки
        time.sleep(0.5)
        processed = i + 1

        # Обновляем прогресс
        self.update_state(
            state="PROGRESS",
            meta={
                "current": processed,
                "total": total,
                "percent": (processed / total) * 100,
                "status": f"Обработано {processed} из {total}"
            }
        )

    return {
        "total": total,
        "processed": processed,
        "status": "completed"
    }



# Пример асинхронной задачи
@celery_app.task(name="tasks.async_example", bind=True)
def async_example(self, url: str) -> Dict:
    """
    Асинхронная задача с aiohttp
    """
    import aiohttp
    import asyncio

    async def fetch():
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                return await response.text()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        result = loop.run_until_complete(fetch())
        return {"url": url, "status": "success", "length": len(result)}
    finally:
        loop.close()


@celery_app.task(name="tasks.parse_category", bind=True)
def parse_category(self, category: str, location: str, quantity: int = None) -> Dict:
    """
    Задача парсинга категории
    """
    from parser.new_parser_card import ParserCard

    logger.info(f"Начинаем парсинг {category} в {location}")

    try:
        parser = ParserCard(category, location, quantity)
        parser.setup_driver()

        # Обновляем прогресс
        self.update_state(
            state="PROGRESS",
            meta={"status": "Парсинг начат", "progress": 0}
        )

        result = parser.parse()

        # Обновляем прогресс
        self.update_state(
            state="PROGRESS",
            meta={"status": "Парсинг завершен", "progress": 100}
        )

        return {
            "category": category,
            "location": location,
            "status": "success",
            "processed": len(result) if result else 0
        }

    except Exception as e:
        logger.error(f"Ошибка парсинга: {e}")
        return {
            "category": category,
            "location": location,
            "status": "failed",
            "error": str(e)
        }
    finally:
        parser.close()
