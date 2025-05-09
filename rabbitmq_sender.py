import pika
import json
import logging
from typing import List, Dict, Any

from config import get_settings

TEST_CDR_EXCHANGE = "cdr-exchange"
TEST_CDR_ROUTING_KEY = "cdr-routing-key"
logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def send_cdr_list_to_rabbitmq(cdr_list: List[Dict[str, Any]]) -> bool:
    if not cdr_list:
        logger.warning("Список CDR для отправки пуст. Отправка отменена.")
        return False

    settings = get_settings()
    try:
        logger.info(f"Подключение к RabbitMQ: host={settings.rabbitmq_host}, port={settings.rabbitmq_port}")
        parameters = pika.ConnectionParameters(
            host=settings.rabbitmq_host,
            port=settings.rabbitmq_port,
            credentials=pika.PlainCredentials(settings.rabbitmq_user, settings.rabbitmq_pass),
            connection_attempts=3,
            retry_delay=5,
        )
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        logger.info("Канал RabbitMQ успешно создан.")

        message_body = json.dumps(cdr_list, ensure_ascii=False).encode('utf-8')
        logger.debug(f"Отправка {len(cdr_list)} CDR записей...")

        channel.basic_publish(
            exchange=TEST_CDR_EXCHANGE,
            routing_key=TEST_CDR_ROUTING_KEY,
            body=message_body,
            properties=pika.BasicProperties(
                content_type='application/json',
                delivery_mode=pika.DeliveryMode.Persistent,
            )
        )
        logger.info(f"Сообщение с {len(cdr_list)} CDR успешно отправлено.")

        return True

    except pika.exceptions.AMQPConnectionError as e:
        logger.error(f"Ошибка подключения к RabbitMQ: {e}")
        return False
    except Exception as e:
        logger.error(f"Неожиданная ошибка при отправке сообщения: {e}", exc_info=True)
        return False
