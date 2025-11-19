import time
import json
from datetime import datetime
from logging import Logger
from typing  import List, Dict

from lib.kafka_connect.kafka_connectors import KafkaConsumer, KafkaProducer
from lib.redis.redis_client import RedisClient
from stg_loader.repository.stg_repository import StgRepository


class StgMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 redis_client: RedisClient,
                 stg_repository: StgRepository,
                 batch_size: int = 100,
                 logger: Logger = None) -> None:
        self._consumer = consumer
        self._producer = producer
        self._redis = redis_client
        self._stg_repository = stg_repository
        self._batch_size = batch_size
        self._logger = logger


    def get_items_info(self, order_items: list, restaurant: dict) -> List[Dict[str, str]]:
        items = []

        menu = restaurant['menu']

        for it in order_items:
            menu_item = next(x for x in menu if x['_id'] == it['id'])
            result = {
                'id': it['id'],
                'price': it['price'],
                'quantity': it['quantity'],
                'name': menu_item['name'],
                'category': menu_item['category']
            }
            items.append(result)
        return items

    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.utcnow()}: START")

        # Имитация работы. Здесь будет реализована обработка сообщений.
        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                self._logger.debug('Сообщений из кафки нет')
                break
            self._logger.debug(f'Получено сообщение из кафки: {msg}')

            # Вставляем сообщениие в postgr
            try:
                self._logger.info('Вставляем сообщение в postgr')

                # Преобразуем данные в нужный формат
                object_id = int(msg['object_id'])
                object_type = msg['object_type']
                payload = json.dumps(msg['payload'])
                sent_dttm = datetime.strptime(msg['sent_dttm'], '%Y-%m-%d %H:%M:%S')

                # Вставляем сообщениие в postgr
                self._stg_repository.order_events_insert(
                        object_id,
                        object_type,
                        sent_dttm,
                        payload
                )

                self._logger.debug(f'Сообщение {object_id} вставлено в stg.order_events')

                # Достаем данные из оперативной памяти облака
                user_id = msg['payload']['user']['id']
                rest_id = msg['payload']['restaurant']['id']

                # Получаем информацию из Redis
                user_info = self._redis.get(user_id)
                self._logger.debug(f'Получили информацию о пользовател из Redis: {user_info}')

                rest_info = self._redis.get(rest_id)
                self._logger.debug(f'Получили информацию о ресторане из Redis: {rest_info}')

                # Формируем итоговое сообщение
                result = {
                    'object_id': object_id,
                    'object_type': object_type,
                    'payload': {
                        'id': object_id,
                        'date': msg['payload']['date'],
                        'cost': msg['payload']['cost'],
                        'payment': msg['payload']['payment'],
                        'status': msg['payload']['final_status'],
                        'restaurant': {
                            'id': rest_info['_id'],
                            'name': rest_info['name']
                        },
                        'user': {
                            'id': user_info['_id'],
                            'name': user_info['name'],
                            'login': user_info['login']
                        },
                        'products': self.get_items_info(msg['payload']['order_items'], rest_info)
                    }
                }

                self._producer.produce(result)
                self._logger.info(f'Сообщение отправлено продюсеру: {result}')

            except Exception as e:
                self._logger.error(f'Ошибка при вставке сообщения: {e}')

        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: FINISH")
