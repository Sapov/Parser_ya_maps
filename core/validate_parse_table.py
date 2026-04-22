from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import sessionmaker
from parser.models import Base
import logging
from core.config import settings

from pydantic import BaseModel, EmailStr

class EmailValidator(BaseModel):
    mail: EmailStr

logger = logging.getLogger(__name__)


class DB:
    '''
    1. Копируем таблицу для работы copy_table_for_work
    2. Удаляем строки с пустыми полями del_blank_srt
    3. Удаляем записи дубликатов email remove_duplicates_in_tables
    4. разбираем поле с несколькимиe mail в разные записи self.split_emails_and_replace()
    5. Валилируем через Pydantic self.clean_and_validate_emails()
    '''

    def __init__(self):
        self.engine = create_engine(
            url=settings.db_url,
            echo=settings.db_echo,
        )
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)
        Base.metadata.create_all(self.engine)
        async_engine = create_async_engine(settings.async_bd_url)
        self.async_session = async_sessionmaker(bind=async_engine, expire_on_commit=False)

    def copy_table_for_work(self, category_id: int, source_table_name, target_table_name='table_temp'):
        """
        Копируем таблицу для пробразований называем table_temp
        """
        with self.engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {target_table_name}"))

            conn.execute(text(f"""
                CREATE TABLE {target_table_name} AS 
                SELECT * FROM {source_table_name} WHERE category_id = {category_id}
            """))

            conn.commit()
            logger.info(f'Сделал копию таблицы {target_table_name} -> {source_table_name}')
            print(f'Сделал копию таблицы {target_table_name} -> {source_table_name}')

    def remove_duplicates_in_tables(self, table_name='table_temp', email_field='mail'):
        """
        Удаляем записи дубликатов email
        """
        with self.engine.connect() as conn:
            # Сначала проверим, есть ли дубликаты
            check = conn.execute(text(f"""
                SELECT {email_field}, COUNT(*) 
                FROM {table_name} 
                GROUP BY {email_field} 
                HAVING COUNT(*) > 1
                LIMIT 5
            """)).fetchall()

            if check:
                print("📋 Найдены дубликаты, примеры:")
                for row in check:
                    print(f"   {row[0]}: {row[1]} раза")

                # Удаляем дубликаты
                delete_sql = text(f"""
                           DELETE FROM {table_name}
                           WHERE id NOT IN (
                               SELECT MIN(id)
                               FROM {table_name}
                               WHERE {email_field} != ''
                               GROUP BY {email_field}
                           )
                           AND {email_field} != ''
                       """)

                result = conn.execute(delete_sql)
                conn.commit()
                print(f"✅ Удалено {result.rowcount} дубликатов")
            else:
                print("✅ Дубликатов не найдено")

            # Показываем итоговую статистику
            total = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
            unique = conn.execute(text(f"SELECT COUNT(DISTINCT {email_field}) FROM {table_name}")).scalar()

            print(f"📊 Итог: {total} записей, {unique} уникальных email")

    def split_emails_and_replace(self, source_table='table_temp', email_field='mail'):
        """
        Разбивает записи с несколькими email на отдельные записи
        и заменяет исходную таблицу на очищенную версию
        """
        temp_table = f"{source_table}_new"

        with self.engine.connect() as conn:
            # 1. Получаем структуру исходной таблицы
            columns_info = conn.execute(text(f"PRAGMA table_info({source_table})")).fetchall()
            columns = [col[1] for col in columns_info if col[1] != 'id']

            # 2. Создаём временную таблицу с автоинкрементным ID
            conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
            create_sql = f"""
                CREATE TABLE {temp_table} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    {', '.join([f'{col} TEXT' for col in columns])}
                )
            """
            conn.execute(text(create_sql))

            # 3. Получаем все записи из источника
            rows = conn.execute(text(f"SELECT * FROM {source_table}")).fetchall()

            inserted_count = 0
            split_count = 0

            for row in rows:
                email_value = str(getattr(row, email_field))

                # Проверяем наличие разделителей (запятая или пробел)
                if ',' in email_value or ' ' in email_value:
                    # Заменяем пробелы на запятые и разбиваем
                    email_value = email_value.replace(' ', ',')
                    emails = [e.strip() for e in email_value.split(',') if e.strip()]
                    split_count += 1

                    for email in emails:
                        # Формируем данные для вставки
                        insert_data = {}
                        for col in columns:
                            if col == email_field:
                                insert_data[col] = email
                            else:
                                insert_data[col] = getattr(row, col)

                        placeholders = ', '.join([f':{col}' for col in insert_data.keys()])
                        columns_str = ', '.join(insert_data.keys())

                        conn.execute(text(f"""
                            INSERT INTO {temp_table} ({columns_str}) 
                            VALUES ({placeholders})
                        """), insert_data)
                        inserted_count += 1
                        print(f"✅ Добавлен email: {email}")
                else:
                    # Записи без разделителей просто копируем
                    insert_data = {}
                    for col in columns:
                        insert_data[col] = getattr(row, col)

                    placeholders = ', '.join([f':{col}' for col in insert_data.keys()])
                    columns_str = ', '.join(insert_data.keys())

                    conn.execute(text(f"""
                        INSERT INTO {temp_table} ({columns_str}) 
                        VALUES ({placeholders})
                    """), insert_data)
                    inserted_count += 1

            conn.commit()

            # 4. Создаём бэкап исходной таблицы (опционально)
            backup_table = f"{source_table}_backup"
            conn.execute(text(f"DROP TABLE IF EXISTS {backup_table}"))
            conn.execute(text(f"ALTER TABLE {source_table} RENAME TO {backup_table}"))
            print(f"📦 Создан бэкап исходной таблицы: {backup_table}")

            # 5. Переименовываем временную таблицу в исходную
            conn.execute(text(f"ALTER TABLE {temp_table} RENAME TO {source_table}"))

            # 6. Удаляем дубликаты в новой таблице (если остались)
            conn.execute(text(f"""
                DELETE FROM {source_table} 
                WHERE id NOT IN (
                    SELECT MIN(id) 
                    FROM {source_table} 
                    GROUP BY {email_field}
                )
            """))

            conn.commit()

            # 7. Статистика
            final_count = conn.execute(text(f"SELECT COUNT(*) FROM {source_table}")).scalar()

            print(f"\n📊 ИТОГО:")
            print(f"   Обработано записей с разделителями: {split_count}")
            print(f"   Создано новых записей: {inserted_count}")
            print(f"   Итоговое количество в таблице {source_table}: {final_count}")

            return inserted_count

    def del_blank_srt(self, table_name='table_temp', mail_field='mail'):
        # удаляем пустые строки из таблицы
        with self.engine.connect() as conn:
            result = conn.execute(text(f"""
                   DELETE FROM {table_name}
                   WHERE {mail_field} = '' OR {mail_field} IS NULL
               """))
            conn.commit()

            print(f"✅ Удалено {result.rowcount} записей с пустыми {mail_field}")
            return result.rowcount



    def clean_and_validate_emails(self, table_name='table_temp'):
        """Очистка и валидация email в таблице"""
        with self.engine.connect() as conn:

            # Получаем все записи
            rows = conn.execute(text(f"SELECT id, mail FROM {table_name}")).fetchall()

            valid_count = 0
            invalid_count = 0

            for row in rows:
                email = row.mail
                if not email:
                    continue

                try:
                    # Валидация email
                    EmailValidator(mail=email)
                    valid_count += 1
                except:
                    # Если email невалидный - помечаем или удаляем
                    print(f"❌ Невалидный email: {email} (ID: {row.id})")
                    conn.execute(text(f"DELETE FROM {table_name} WHERE id = {row.id}"))
                    invalid_count += 1
            conn.commit()

            logger.info(f"\n✅ Валидных: {valid_count}")
            logger.info(f"❌ Невалидных: {invalid_count}")
            print(f"\n✅ Валидных: {valid_count}")
            print(f"❌ Невалидных: {invalid_count}")



    def run(self):
        self.copy_table_for_work(1, 'Organisations')
        self.del_blank_srt()
        self.split_emails_and_replace()
        self.remove_duplicates_in_tables()
        self.clean_and_validate_emails()



if __name__ == '__main__':
    db = DB()
    db.run()
