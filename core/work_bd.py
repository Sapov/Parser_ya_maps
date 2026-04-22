from sqlalchemy import select, and_, or_, func, delete

from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import sessionmaker
from parser.models import Base
import logging
from core.config import settings

logger = logging.getLogger(__name__)


class DB:
    '''
    1. Копируем таблицу для работы copy_table_for_work
    2. Удаляем записи дубликатов email
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

    def copy_table_for_work(self, category_id: int, source_table_name, target_table_name='table_temp' ):
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
            # Создаём временную таблицу без дубликатов
            temp_table = f"{table_name}_temp"

            sql_create = text(f"""
                CREATE TABLE {temp_table} AS
                SELECT * FROM {table_name} 
                WHERE id IN (
                    SELECT MIN(id) 
                    FROM {table_name} 
                    GROUP BY {email_field}
                )
            """)

            # Удаляем старую таблицу
            sql_drop = text(f"DROP TABLE {table_name}")
            #
            # Переименовываем временную
            sql_rename = text(f"ALTER TABLE {temp_table} RENAME TO {table_name}")

            conn.execute(sql_create)
            conn.execute(sql_drop)
            conn.execute(sql_rename)
            conn.commit()

            print(f"✅ Таблица {table_name} очищена от дубликатов")

    def find_emails_with_comma(self, table_name, mail_field='mail'):
        """
        Находит все записи, где в поле mail есть запятая
        """
        with self.engine.connect() as conn:
            sql = text(f"""
                SELECT * FROM {table_name} 
                WHERE {mail_field} LIKE '%,%'
            """)

            results = conn.execute(sql).fetchall()

            print(f"Найдено {len(results)} записей с запятой в поле {mail_field}")
            return results

    def split_emails_to_new_table(self, source_table, target_table, email_field='mail'):
        """
        Находим все записи где есть дреса mail через запятую
        Разбиваем записи с несколькими email (через запятую) на отдельные записи
        """
        with self.engine.connect() as conn:
            # Создаём новую таблицу
            conn.execute(text(f"DROP TABLE IF EXISTS {target_table}"))
            conn.execute(text(f"""
                CREATE TABLE {target_table} AS 
                SELECT * FROM {source_table} WHERE 1=0
            """))

            # Получаем все записи с запятыми
            sql = text(f"SELECT * FROM {source_table} WHERE {email_field} LIKE '%,%'")
            rows_with_comma = conn.execute(sql).fetchall()

            inserted_count = 0

            for row in rows_with_comma:
                # Разбиваем email по запятой
                emails = str(getattr(row, email_field)).split(',')

                for email in emails:
                    email = email.strip()  # Убираем пробелы
                    if email:  # Проверяем, что email не пустой
                        # Создаём новую запись
                        insert_sql = text(f"""
                            INSERT INTO {target_table} 
                            SELECT * FROM {source_table} 
                            WHERE id = :id
                        """)
                        conn.execute(insert_sql, {"id": row.id})

                        # Обновляем email в новой записи
                        update_sql = text(f"""
                            UPDATE {target_table} 
                            SET {email_field} = :email 
                            WHERE id = (SELECT MAX(id) FROM {target_table})
                        """)
                        conn.execute(update_sql, {"email": email})

                        inserted_count += 1
                        print(f"✅ Добавлен email: {email}")

            conn.commit()
            print(f"\n📊 Итого: {inserted_count} новых записей создано")
            return inserted_count

    def run(self):
        # self.copy_table_for_work(2, 'Organisations')
        self.remove_duplicates_in_tables()

if __name__ == '__main__':
    db = DB()
    db.run()

    # db.remove_duplicates_safe('Organisations_copy')
    # db.find_emails_with_comma('Organisations_copy')
    # db.split_emails_to_new_table('Organisations_copy', 'mail_organisations')
