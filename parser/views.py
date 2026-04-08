from fastapi import APIRouter

from core.db import DB

router = APIRouter(prefix='/parser', tags=['parser'])

from tasks import parse, parser_all_city


@router.post("/api/tasks/process")
async def create_process_task(category: str, city: str, quantity: int):
    """
    Запускает парсинг яндекс карт: \n
    Нужно указать:\n
    category - категория например Рестораны\n
    city - город\n
    quantity - количество позиций
    """
    task = parse.delay(category, city, quantity)

    return {
        "task_id": task.id,
        "status": "started",
        "message": f"Задача запущена: {category} - {city}"
    }


@router.post('/passing_all_city')
async def passing_all_city(category: str):
    '''
    Парсинг категории по всем городам
    :param category:
    :return:
    '''
    task = parser_all_city.delay(category)
    return {
        "task_id": task.id,
        "status": "started",
        "message": f"Задача парсинг по всем городам категории: {category} запущена"
    }


@router.get('/city')
async def get_city():
    '''
    Все города России
    :return:
    '''
    db = DB()
    citys = db.get_city()
    return {
        "citys": citys,
        "status": "completed"
    }
