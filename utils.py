from datetime import datetime
import math
import psycopg
import logging

logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def connect_db(db_name, db_url) -> psycopg.Connection | None:
    try:
        conn = psycopg.connect(db_url, autocommit=False)
        logger.info(
            f"Успешное подключение к БД {db_name}"
        )
        return conn
    except psycopg.OperationalError as e:
        logger.error(f"Ошибка подключения к БД: {e}")
        return None
    except Exception as e:
        logger.error(f"Неожиданная ошибка при подключении к БД: {e}", exc_info=True)
        return None


def close_db(conn: psycopg.Connection | None):
    if conn and not conn.closed:
        try:
            if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
                conn.rollback()
                logger.warning("Откат незавершенной транзакции при закрытии соединения.")
            conn.close()
            logger.info("Соединение с БД закрыто.")
        except Exception as e:
            logger.error(f"Ошибка при закрытии соединения с БД: {e}", exc_info=True)


def calculate_billed_minutes(call_start_str: str, call_end_str: str) -> int:
    start_time = datetime.fromisoformat(call_start_str)
    end_time = datetime.fromisoformat(call_end_str)
    duration_timedelta = end_time - start_time
    duration_seconds = duration_timedelta.total_seconds()
    billed_minutes = math.ceil(duration_seconds / 60.0)
    return int(billed_minutes)
