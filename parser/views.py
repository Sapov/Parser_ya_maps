
from fastapi import APIRouter

from core.tasks import search_itm
router = APIRouter(prefix='/parser', tags=['parser'])




# @router.post('/run/')
# def run_parser(category: str, city: str, quantity:int):
#     search = ParserCard(category, city, quantity)
#     search.run()
#     ParserPage().run()
#     item = DB()
#     lst_old = item.get_all_sites()
#     lst = asyncio.run(ParseSite(lst_old).main())
#     save_data(lst)
#     return 'wait'


@router.post("/api/tasks/process")
async def create_process_task(category: str, city: str, quantity:int):
    """
    Запускает задачу обработки данных
    """
    task = search_itm.delay(category, city, quantity)
    return {
        "task_id": task.id,
        "status": "started",
        "message": "Задача обработки запущена"
    }