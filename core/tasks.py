import time
import asyncio
from typing import Dict, Any
from celery import shared_task

from .celery_worker import celery_app
import logging


logger = logging.getLogger(__name__)


# Базовый пример простой задачи
@celery_app.task(name="tasks.search")
def search_itm(category: str, city: str, quantity: int) -> list:
    lst = run()
    save_data(lst)
    return lst


    return lst

#
# # Задача с прогрессом
# @celery_app.task(name="tasks.process_data", bind=True)
# def process_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
#     """Задача с отслеживанием прогресса"""
#     total_steps = len(data.get("items", []))
#
#     for i, item in enumerate(data.get("items", [])):
#         # Имитация обработки
#         time.sleep(1)
#         # Обновляем прогресс (доступно через Flower или API)
#         self.update_state(
#             state="PROGRESS",
#             meta={
#                 "current": i + 1,
#                 "total": total_steps,
#                 "status": f"Обработано {i + 1} из {total_steps} элементов"
#             }
#         )
#
#     return {"status": "completed", "processed": total_steps}
#
#
# # Асинхронная задача (для асинхронных операций)
# @celery_app.task(name="tasks.async_operation")
# def async_operation(data: Dict[str, Any]) -> Dict[str, Any]:
#     """Задача с асинхронными операциями"""
#     # Для асинхронных операций используем asyncio.run()
#     # Или делаем синхронную обертку
#     loop = asyncio.new_event_loop()
#     asyncio.set_event_loop(loop)
#     try:
#         result = loop.run_until_complete(_async_work(data))
#     finally:
#         loop.close()
#     return result
#
#
# async def _async_work(data: Dict[str, Any]) -> Dict[str, Any]:
#     """Асинхронная работа"""
#     await asyncio.sleep(3)
#     return {"result": "async work done", "data": data}
#
#
# # Задача для работы с базой данных
# @celery_app.task(name="tasks.save_to_db", bind=True)
# def save_to_db(self, items: list) -> Dict[str, Any]:
#     """Сохранение данных в базу"""
#     from app.db import SessionLocal
#     from app.models import Item
#
#     session = SessionLocal()
#     try:
#         saved_count = 0
#         for item_data in items:
#             # Проверка существования
#             existing = session.query(Item).filter_by(link=item_data["link"]).first()
#             if not existing:
#                 item = Item(**item_data)
#                 session.add(item)
#                 saved_count += 1
#
#             # Обновляем прогресс
#             self.update_state(
#                 state="PROGRESS",
#                 meta={"saved": saved_count, "total": len(items)}
#             )
#
#         session.commit()
#         return {"saved": saved_count, "total": len(items)}
#     except Exception as e:
#         session.rollback()
#         raise e
#     finally:
#         session.close()
