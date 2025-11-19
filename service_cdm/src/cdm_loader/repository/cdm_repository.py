from typing import Dict

from lib.pg import PgConnect
import hashlib

class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def _insert(self, *, table_name: str, data: Dict, conflict_fields: list) -> None:
        """
        Универсальный метод для вставки данных в конкретную таблицу.
        Args:
            table_name: Название таблицы
            data: Данные dict, которые нужно вставить
            conflict_fields: Список атрибутов, которые участвуют в конфликте при вставке в sql
        """
        keys = ', '.join(data.keys())
        values = ', '.join([f'%({key})s' for key in data.keys()])
        conflict_values = ', '.join(conflict_fields)

        sql = f"""
            INSERT INTO cdm.{table_name} ({keys})
            VALUES ({values})
            ON CONFLICT ({conflict_values}) DO UPDATE
            SET order_cnt = {table_name}.order_cnt + EXCLUDED.order_cnt;
        """

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, data)

    def insert_to_user_category_counters(self, data: Dict) -> None:
        """
        Метод готовит данные для вставки в таблицу user_category_counters,
        затем передаёт подготовленные данные в универсальный метод _insert.
        """
        status = str(data['status']).lower()
        user_id = data['user']['id']
        user_hk = hashlib.md5(user_id.encode('utf-8')).hexdigest()

        # Обрабатываем только завершенные заказы
        if status == 'closed':
            for item in data['products']:
                category_name = item['category']
                category_id = hashlib.md5(category_name.encode('utf-8')).hexdigest()
                order_cnt = item['quantity']

                # В данной реализации sql код у нас жестко привязан к полю order_cnt, но в будущем можно передавать
                # атрибут, к которому будет применяться инкремент
                data = {
                    'user_id': user_hk,
                    'category_id': category_id,
                    'category_name': category_name,
                    'order_cnt': int(order_cnt)
                }
                self._insert(
                    table_name='user_category_counters',
                    data=data,
                    conflict_fields=['user_id', 'category_id']
                    )

    def insert_to_user_product_counters(self, data: Dict):
        """
        Метод готовит данные для вставки в таблицу user_product_counters,
        затем передаёт подготовленные данные в универсальный метод _insert.
        """
        status = str(data['status']).lower()
        user_id = data['user']['id']
        user_hk = hashlib.md5(user_id.encode('utf-8')).hexdigest()

        if status == 'closed':
            for item in data['products']:
                product_id = item['id']
                product_hk = hashlib.md5(product_id.encode('utf-8')).hexdigest()
                product_name = item['name']
                order_cnt = item['quantity']
                data = {
                    'user_id': user_hk,
                    'product_id': product_hk,
                    'product_name': product_name,
                    'order_cnt': int(order_cnt)
                }
                self._insert(
                    table_name='user_product_counters',
                    data=data,
                    conflict_fields=['user_id', 'product_id']
                )
