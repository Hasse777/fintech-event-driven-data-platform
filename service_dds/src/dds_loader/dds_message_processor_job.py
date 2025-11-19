import time
import json
from datetime import datetime
from logging import Logger
from typing  import List, Dict

from lib.kafka_connect.kafka_connectors import KafkaConsumer, KafkaProducer
from dds_loader.repository.dds_repository import DdsRepository


class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 batch_size: int = 100,
                 logger: Logger = None) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._batch_size = batch_size
        self._logger = logger

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
            try:

                # Вставляем сообщениие в postgr
                self._logger.info('Вставляем сообщение в postgr')

                self._logger.debug('Начинаем вставлять данные в h_user')
                self._dds_repository.insert_h_user(msg=msg)
                self._logger.debug('Данные загружены в h_user')

                self._logger.debug('Начинаем вставлять данные в h_product')
                self._dds_repository.insert_h_product(msg=msg)
                self._logger.debug('Данные загружены в h_product')

                self._logger.debug('Начинаем вставлять данные в h_category')
                self._dds_repository.insert_h_category(msg=msg)
                self._logger.debug('Данные загружены в h_category')

                self._logger.debug('Начинаем вставлять данные в h_restaurant')
                self._dds_repository.insert_h_restaurant(msg=msg)
                self._logger.debug('Данные загружены в h_restaurant')

                self._logger.debug('Начинаем вставлять данные в h_order')
                self._dds_repository.insert_h_order(msg=msg)
                self._logger.debug('Данные загружены в h_order')

                self._logger.debug('Начинаем вставлять данные в l_order_product')
                self._dds_repository.insert_l_order_product(msg=msg)
                self._logger.debug('Данные загружены в l_order_product')

                self._logger.debug('Начинаем вставлять данные в l_product_restaurant')
                self._dds_repository.insert_l_product_restaurant(msg=msg)
                self._logger.debug('Данные загружены в l_product_restaurant')

                self._logger.debug('Начинаем вставлять данные в l_product_category')
                self._dds_repository.insert_l_product_category(msg=msg)
                self._logger.debug('Данные загружены в l_product_category')

                self._logger.debug('Начинаем вставлять данные в l_order_user')
                self._dds_repository.insert_l_order_user(msg=msg)
                self._logger.debug('Данные загружены в l_order_user')

                self._logger.debug('Начинаем вставлять данные в s_user_names')
                self._dds_repository.insert_s_user_names(msg=msg)
                self._logger.debug('Данные загружены в s_user_names')

                self._logger.debug('Начинаем вставлять данные в s_product_names')
                self._dds_repository.insert_s_product_names(msg=msg)
                self._logger.debug('Данные загружены в s_product_names')

                self._logger.debug('Начинаем вставлять данные в s_restaurant_names')
                self._dds_repository.insert_s_restaurant_names(msg=msg)
                self._logger.debug('Данные загружены в s_restaurant_names')

                self._logger.debug('Начинаем вставлять данные в s_order_cost')
                self._dds_repository.insert_s_order_cost(msg=msg)
                self._logger.debug('Данные загружены в s_order_cost')

                self._logger.debug('Начинаем вставлять данные в s_order_status')
                self._dds_repository.insert_s_order_status(msg=msg)
                self._logger.debug('Данные загружены в s_order_status')

                self._logger.info('Все данные загружены в таблицы')
                # ----------------------------------------------------------------------------

                # Готовим сообщения для отправки в кафку
                result = {
                    'object_id': msg['object_id'],
                    'object_type': msg['object_type'],
                    'status': msg['payload']['status'],
                    'date': msg['payload']['date'],
                    'user': msg['payload']['user'],
                    'products': msg['payload']['products'],

                }
                self._producer.produce(result)
                self._logger.info(f'DDS Сообщение отправлено продюсеру: {result}')

            except Exception as e:
                self._logger.error(f'Ошибка при обработке сообщения: {e}')

        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: FINISH")
