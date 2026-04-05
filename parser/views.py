from itertools import cycle

from fastapi import APIRouter

router = APIRouter(prefix='/parser', tags=['parser'])

from tasks import example_hello, add_numbers, parse


@router.post("/api/tasks/process")
async def create_process_task(category: str, city: str, quantity:int):
    """
    Запускает задачу обработки данных
    """
    print('VIEW', category, city, quantity)
    task = parse.delay(category, city, quantity)
    return {
        "task_id": task.id,
        "status": "started",
        "message": "Задача обработки запущена"
    }



# Эндпоинты для работы с задачами
@router.post("/api/tasks/hello")
async def create_hello_task(name):
    """
    Создание задачи Hello
    """
    task = example_hello.delay(name)
    return {
        "task_id": task.id,
        "status": "pending",
        "message": f"Задача создана для {name}"
    }

@router.post("/api/tasks/add")
async def create_add_task(x: int, y:float):
    """
    Создание задачи сложения
    """
    task = add_numbers.delay(x, y)
    return {
        "task_id": task.id,
        "status": "pending",
        "message": f"Задача на сложение {x} + {y} создана"
    }