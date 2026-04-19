from fastapi import APIRouter

router = APIRouter(prefix='/data', tags=['data'])

import csv
import re
from fastapi import APIRouter, Depends, Query, HTTPException
from datetime import datetime

from core.db import DB



def validate_email(email: str) -> bool:
    """Проверяет валидность email"""
    if not email or email == "":
        return False
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))


def reformat_with_email_validation(lst: list, only_valid_email: bool = False) -> list:
    """
    Реформатирует данные для CSV с валидацией email

    Args:
        lst: Список организаций из БД
        only_valid_email: Если True, пропускает записи с невалидным email

    Returns:
        Список строк для CSV
    """
    r_lst = []
    title = ['email', 'name', 'attributes']
    r_lst.append(title)

    for org in lst:
        # Пропускаем если нет email
        if not org.mail or org.mail == "":
            continue

        # Проверка валидности email
        if only_valid_email and not validate_email(org.mail):
            continue

        # Обрабатываем множественные email (разделенные запятой)
        if ', ' in org.mail:
            for email in org.mail.split(', '):
                if only_valid_email and not validate_email(email):
                    continue
                r_lst.append([email.strip(), org.title, ''])
        else:
            r_lst.append([org.mail.strip(), org.title, ''])

    return r_lst




@router.get("/csv/listmonk/save-to-file/")
async def export_and_save_to_file(
        category: str = Query(..., description="Название категории"),
        only_valid_email: bool = Query(True, description="Только валидные email")
):
    """
    Сохраняет CSV файл на диск и отдаёт ссылку для скачивания.
    """
    import os
    from fastapi.responses import FileResponse

    db = DB()
    organisations = db.category_select_with_email(category)

    if not organisations:
        raise HTTPException(status_code=404, detail=f"Нет данных для категории: {category}")

    # Реформатируем данные
    data = reformat_with_email_validation(organisations, only_valid_email)

    if len(data) <= 1:  # Только заголовок
        raise HTTPException(status_code=404, detail="Нет валидных email для экспорта")

    # Сохраняем на диск
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"listmonk_{category}_{timestamp}.csv"
    filepath = f"/tmp/{filename}"  # или в папку ./exports/

    # Создаём директорию если её нет
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    with open(filepath, 'w', newline='', encoding='utf-8-sig') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerows(data)

    return FileResponse(
        path=filepath,
        media_type="text/csv",
        filename=filename,
        background=None  # можно добавить удаление файла после отправки
    )