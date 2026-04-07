from fastapi import APIRouter

router = APIRouter(prefix='/parser', tags=['parser'])

from tasks import parse


@router.post("/api/tasks/process")
async def create_process_task(category: str, city: str, quantity:int):
    """
    Запускает парсинг яндекс карт: \n
    Нужно указать:\n
    category - категория например Рестораны\n
    city - город\n
    quantity - количество позиций
    """
    task = parse.delay(category, city, quantity )



    return {
        "task_id": task.id,
        "status": "started",
        "message": f"Задача запущена: {category} - {city}"
    }