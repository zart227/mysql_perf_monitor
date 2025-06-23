import logging
import os
from datetime import datetime
from config.config import LOG_TO_FILE, LOG_TO_CONSOLE, LOG_LEVEL

LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

log_file = os.path.join(LOG_DIR, f"mysql_perf_reporter_{datetime.now().strftime('%Y%m%d')}.log")

# Создаем логгер
logger = logging.getLogger('mysql_perf_reporter')
logger.setLevel(getattr(logging, LOG_LEVEL))

# Очищаем существующие обработчики
for handler in logger.handlers[:]:
    logger.removeHandler(handler)

# Создаем форматтер
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')

# Добавляем обработчик для файла (если включено)
if LOG_TO_FILE:
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

# Добавляем обработчик для консоли (если включено)
if LOG_TO_CONSOLE:
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

# Если ни один обработчик не добавлен, добавляем NullHandler
if not logger.handlers:
    logger.addHandler(logging.NullHandler())

# Не логировать SMTP_PASSWORD и другие секреты
logging.getLogger('smtplib').setLevel(logging.WARNING) 