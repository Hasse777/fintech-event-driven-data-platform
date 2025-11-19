from datetime import datetime
from logging import Logger

from lib.kafka_connect.kafka_connectors import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository


class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 batch_size: int = 100,
                 logger: Logger = None) -> None:
        self._consumer = consumer
        self._cdm_repository = cdm_repository
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
                self._logger.debug('Начинаем вставлять данные в таблицу user_category_counters')
                self._cdm_repository.insert_to_user_category_counters(msg)
                self._logger.debug('Данные загружены в таблицу user_category_counters')

                self._logger.debug('Начинаем вставлять данные в таблицу user_product_counters')
                self._cdm_repository.insert_to_user_product_counters(msg)
                self._logger.debug('Данные загружены в таблицу user_product_counters')

                self._logger.debug('Все данные успешно загружены')

            except Exception as e:
                self._logger.error(f'Ошибка при обработке сообщения: {e}')

        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: FINISH")
