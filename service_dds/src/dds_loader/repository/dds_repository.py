from datetime import datetime
from typing import Dict
from decimal import Decimal

from lib.pg import PgConnect
import hashlib

class DdsRepository:
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

        # Если при выполнении запроса случится конфликт, нужна будет специальная строчка
        # Формируем её
        update_clause = ', '.join([f'{key} = EXCLUDED.{key}' for key in data.keys() if key not in conflict_fields])
        sql = f"""
            INSERT INTO dds.{table_name} ({keys})
            VALUES ({values})
            ON CONFLICT ({conflict_values}) DO UPDATE
            SET {update_clause};
        """

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, data)

    def insert_h_user(self, *, msg: dict, load_src: str = 'stg-service-orders'):
        """
        Метод готовит данные для вставки в таблицу h_user и передает преобразованные данные
        в универсальный метод _insert.
        """
        user_id = str(msg['payload']['user']['id'])

        data = {
            'h_user_pk': hashlib.md5(user_id.encode('utf-8')).hexdigest(),
            'user_id': msg['payload']['user']['id'],
            'load_dt': datetime.now(),
            'load_src': load_src
        }
        self._insert(
            table_name='h_user',
            data=data,
            conflict_fields=['h_user_pk']
            )

    def insert_h_product(self, *, msg: dict, load_src: str = 'stg-service-orders'):
        """
        Метод готовит данные для вставки в таблицу h_product и передает преобразованные данные
        в универсальный метод _insert.
        """
        for item in msg['payload']['products']:
            product_id = str(item['id'])
            data = {
                'h_product_pk': hashlib.md5(product_id.encode('utf-8')).hexdigest(),
                'product_id': product_id,
                'load_dt': datetime.now(),
                'load_src': load_src
            }
            self._insert(
                table_name='h_product',
                data=data,
                conflict_fields=['h_product_pk']
            )

    def insert_h_category(self, *, msg: dict, load_src: str = 'stg-service-orders'):
        """
        Метод готовит данные для вставки в таблицу h_category и передает преобразованные данные
        в универсальный метод _insert.
        """
        for item in msg['payload']['products']:
            category_name = str(item['category'])
            data = {
                'h_category_pk': hashlib.md5(category_name.encode('utf-8')).hexdigest(),
                'category_name': category_name,
                'load_dt': datetime.now(),
                'load_src': load_src
            }
            self._insert(
                table_name='h_category',
                data=data,
                conflict_fields=['h_category_pk']
            )

    def insert_h_restaurant(self, *, msg: dict, load_src: str = 'stg-service-orders'):
        """
        Метод готовит данные для вставки в таблицу h_restaurant и передает преобразованные данные
        в универсальный метод _insert.
        """
        restaurant_id = str(msg['payload']['restaurant']['id'])
        data = {
            'h_restaurant_pk': hashlib.md5(restaurant_id.encode('utf-8')).hexdigest(),
            'restaurant_id': restaurant_id,
            'load_dt': datetime.now(),
            'load_src': load_src
        }
        self._insert(
                table_name='h_restaurant',
                data=data,
                conflict_fields=['h_restaurant_pk']
            )

    def insert_h_order(self, *, msg: dict, load_src: str = 'stg-service-orders'):
        """
        Метод готовит данные для вставки в таблицу h_order и передает преобразованные данные
        в универсальный метод _insert.
        """
        order_id = int(msg['payload']['id'])
        order_dt = datetime.strptime(msg['payload']['date'], '%Y-%m-%d %H:%M:%S')
        data = {
            'h_order_pk': hashlib.md5(str(order_id).encode('utf-8')).hexdigest(),
            'order_id': order_id,
            'order_dt': order_dt,
            'load_dt': datetime.now(),
            'load_src': load_src
        }
        self._insert(
                table_name='h_order',
                data=data,
                conflict_fields=['h_order_pk']
            )

    def insert_l_order_product(self, *, msg: dict, load_src: str = 'stg-service-orders'):
        """
        Метод готовит данные для вставки в таблицу l_order_product и передает преобразованные данные
        в универсальный метод _insert.
        """
        order_id = int(msg['payload']['id'])
        h_order_pk = hashlib.md5(str(order_id).encode('utf-8')).hexdigest()

        for item in msg['payload']['products']:
            product_id = str(item['id'])
            h_product_pk = hashlib.md5(product_id.encode('utf-8')).hexdigest()
            composite_key = h_order_pk + h_product_pk
            data = {
                'hk_order_product_pk': hashlib.md5(composite_key.encode('utf-8')).hexdigest(),
                'h_product_pk': h_product_pk,
                'h_order_pk': h_order_pk,
                'load_dt': datetime.now(),
                'load_src': load_src
            }
            self._insert(
                table_name='l_order_product',
                data=data,
                conflict_fields=['hk_order_product_pk']
            )

    def insert_l_product_restaurant(self, *, msg: dict, load_src: str = 'stg-service-orders'):
        """
        Метод готовит данные для вставки в таблицу l_product_restaurant и передает преобразованные данные
        в универсальный метод _insert.
        """
        restaurant_id = str(msg['payload']['restaurant']['id'])

        for item in msg['payload']['products']:
            product_id = str(item['id'])

            h_product_pk = hashlib.md5(product_id.encode('utf-8')).hexdigest()
            h_restaurant_pk = hashlib.md5(restaurant_id.encode('utf-8')).hexdigest()
            composite_key = h_product_pk + h_restaurant_pk

            data = {
                'hk_product_restaurant_pk': hashlib.md5(composite_key.encode('utf-8')).hexdigest(),
                'h_product_pk': h_product_pk,
                'h_restaurant_pk': h_restaurant_pk,
                'load_dt': datetime.now(),
                'load_src': load_src
            }
            self._insert(
                table_name='l_product_restaurant',
                data=data,
                conflict_fields=['hk_product_restaurant_pk']
            )

    def insert_l_product_category(self, *, msg: dict, load_src: str = 'stg-service-orders'):
        """
        Метод готовит данные для вставки в таблицу l_product_category и передает преобразованные данные
        в универсальный метод _insert.
        """
        for item in msg['payload']['products']:
            product_id = str(item['id'])
            category_name = str(item['category'])

            h_product_pk = hashlib.md5(product_id.encode('utf-8')).hexdigest()
            h_category_pk = hashlib.md5(category_name.encode('utf-8')).hexdigest()
            composite_key = h_product_pk + h_category_pk

            data = {
                'hk_product_category_pk': hashlib.md5(composite_key.encode('utf-8')).hexdigest(),
                'h_product_pk': h_product_pk,
                'h_category_pk': h_category_pk,
                'load_dt': datetime.now(),
                'load_src': load_src
            }
            self._insert(
                table_name='l_product_category',
                data=data,
                conflict_fields=['hk_product_category_pk']
            )

    def insert_l_order_user(self, *, msg: dict, load_src: str = 'stg-service-orders'):
        """
        Метод готовит данные для вставки в таблицу l_order_user и передает преобразованные данные
        в универсальный метод _insert.
        """
        user_id = str(msg['payload']['user']['id'])
        order_id = int(msg['payload']['id'])

        h_user_pk = hashlib.md5(user_id.encode('utf-8')).hexdigest()
        h_order_pk = hashlib.md5(str(order_id).encode('utf-8')).hexdigest()
        composite_key = h_user_pk + h_order_pk

        data = {
                'hk_order_user_pk': hashlib.md5(composite_key.encode('utf-8')).hexdigest(),
                'h_user_pk': h_user_pk,
                'h_order_pk': h_order_pk,
                'load_dt': datetime.now(),
                'load_src': load_src
            }
        self._insert(
                table_name='l_order_user',
                data=data,
                conflict_fields=['hk_order_user_pk']
            )

    def insert_s_user_names(self, *, msg: dict, load_src: str = 'stg-service-orders'):
        """
        Метод готовит данные для вставки в таблицу s_user_names и передает преобразованные данные
        в универсальный метод _insert.
        """
        user_id = str(msg['payload']['user']['id'])
        username = str(msg['payload']['user']['name'])
        userlogin = str(msg['payload']['user']['login'])
        h_user_pk = hashlib.md5(user_id.encode('utf-8')).hexdigest()

        composite_key = h_user_pk + username + userlogin
        data = {
                'hk_user_names_hashdiff': hashlib.md5(composite_key.encode('utf-8')).hexdigest(),
                'h_user_pk': h_user_pk,
                'username': username,
                'userlogin': userlogin,
                'load_dt': datetime.now(),
                'load_src': load_src
            }
        self._insert(
                table_name='s_user_names',
                data=data,
                conflict_fields=['hk_user_names_hashdiff']
            )

    def insert_s_product_names(self, *, msg: dict, load_src: str = 'stg-service-orders'):
        """
        Метод готовит данные для вставки в таблицу s_product_names и передает преобразованные данные
        в универсальный метод _insert.
        """
        for item in msg['payload']['products']:
            product_id = str(item['id'])
            product_name = str(item['name'])
            h_product_pk = hashlib.md5(product_id.encode('utf-8')).hexdigest()

            composite_key = h_product_pk + product_name

            data = {
                'hk_product_names_hashdiff': hashlib.md5(composite_key.encode('utf-8')).hexdigest(),
                'h_product_pk': h_product_pk,
                'name': product_name,
                'load_dt': datetime.now(),
                'load_src': load_src
            }
            self._insert(
                table_name='s_product_names',
                data=data,
                conflict_fields=['hk_product_names_hashdiff']
            )

    def insert_s_restaurant_names(self, *, msg: dict, load_src: str = 'stg-service-orders'):
        """
        Метод готовит данные для вставки в таблицу s_restaurant_names и передает преобразованные данные
        в универсальный метод _insert.
        """
        restaurant_id = str(msg['payload']['restaurant']['id'])
        restaurant_name = str(msg['payload']['restaurant']['name'])
        h_restaurant_pk = hashlib.md5(restaurant_id.encode('utf-8')).hexdigest()

        composite_key = h_restaurant_pk + restaurant_name

        data = {
                'hk_restaurant_names_hashdiff': hashlib.md5(composite_key.encode('utf-8')).hexdigest(),
                'h_restaurant_pk': h_restaurant_pk,
                'name': restaurant_name,
                'load_dt': datetime.now(),
                'load_src': load_src
            }
        self._insert(
                table_name='s_restaurant_names',
                data=data,
                conflict_fields=['hk_restaurant_names_hashdiff']
            )

    def insert_s_order_cost(self, *, msg: dict, load_src: str = 'stg-service-orders'):
        """
        Метод готовит данные для вставки в таблицу s_order_cost и передает преобразованные данные
        в универсальный метод _insert.
        """
        order_id = int(msg['payload']['id'])
        cost = Decimal(msg['payload']['cost'])
        payment = Decimal(msg['payload']['payment'])

        h_order_pk = hashlib.md5(str(order_id).encode('utf-8')).hexdigest()

        composite_key = f'{h_order_pk}|{cost}|{payment}'

        data = {
                'hk_order_cost_hashdiff': hashlib.md5(composite_key.encode('utf-8')).hexdigest(),
                'h_order_pk': h_order_pk,
                'cost': cost,
                'payment': payment,
                'load_dt': datetime.now(),
                'load_src': load_src
            }
        self._insert(
                table_name='s_order_cost',
                data=data,
                conflict_fields=['hk_order_cost_hashdiff']
            )

    def insert_s_order_status(self, *, msg: dict, load_src: str = 'stg-service-orders'):
        """
        Метод готовит данные для вставки в таблицу s_order_status и передает преобразованные данные
        в универсальный метод _insert.
        """
        order_id = int(msg['payload']['id'])
        h_order_pk = hashlib.md5(str(order_id).encode('utf-8')).hexdigest()
        status = str(msg['payload']['status'])

        composite_key = h_order_pk + status
        data = {
                'hk_order_status_hashdiff': hashlib.md5(composite_key.encode('utf-8')).hexdigest(),
                'h_order_pk': h_order_pk,
                'status': status,
                'load_dt': datetime.now(),
                'load_src': load_src
            }
        self._insert(
                table_name='s_order_status',
                data=data,
                conflict_fields=['hk_order_status_hashdiff']
            )
