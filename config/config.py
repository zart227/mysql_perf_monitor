import os
from dotenv import load_dotenv

# Загружаем переменные окружения из .env файла
load_dotenv()

# --- Базовая конфигурация путей ---
# BASE_DIR - это корневая директория проекта (mysql_perf_monitor)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# SSH параметры
SSH_HOST = os.getenv('SSH_HOST')
SSH_PORT = int(os.getenv('SSH_PORT', 22))
SSH_USER = os.getenv('SSH_USER')
SSH_PASSWORD = os.getenv('SSH_PASSWORD')
SSH_HOSTKEY_ALG = os.getenv('SSH_HOSTKEY_ALG')
SSH_PUBKEY_TYPES = os.getenv('SSH_PUBKEY_TYPES')

SSH_CONFIG = {
    'host': os.getenv('SSH_HOST', '10.10.40.79'),
    'port': int(os.getenv('SSH_PORT', 22)),
    'user': os.getenv('SSH_USER', 'logs'),
    'password': os.getenv('SSH_PASSWORD', 'your_password'),
    'hostkey_algorithms': SSH_HOSTKEY_ALG,
    'pubkey_accepted_key_types': SSH_PUBKEY_TYPES,
}

# MySQL конфигурация
MYSQL_CONFIG = {
    'user': os.getenv('MYSQL_USER', 'smiths'),
    'password': os.getenv('MYSQL_PASSWORD', 'cvbnc'),
    'host': os.getenv('MYSQL_HOST', 'localhost'),
    'port': int(os.getenv('MYSQL_PORT', 3306)),
    'database': os.getenv('MYSQL_DB', '')
}

# Временные окна мониторинга (24-часовой формат)
MONITOR_WINDOWS_ENABLED = os.getenv('MONITOR_WINDOWS_ENABLED', 'True').lower() in ('true', '1', 't')
MONITOR_WINDOWS = [
    {'start': '05:00', 'end': '07:00'},
    {'start': '22:00', 'end': '01:00'},
]

# Email настройки
EMAIL_ENABLED = os.getenv('EMAIL_ENABLED', 'False').lower() == 'true'
SMTP_SERVER = os.getenv('SMTP_SERVER', '')
SMTP_PORT = int(os.getenv('SMTP_PORT', 587))
SMTP_USER = os.getenv('SMTP_USER', '')
SMTP_PASSWORD = os.getenv('SMTP_PASSWORD', '')
FROM_ADDR = os.getenv('FROM_ADDR', '')
TO_ADDRS = [addr.strip() for addr in os.getenv('TO_ADDRS', '').split(',') if addr.strip()]

# ВНИМАНИЕ: Пароль будет виден в списке процессов на удаленном сервере.
mysql_conn_string = (
    f"mysql -u'{MYSQL_CONFIG['user']}' "
    f"-p'{MYSQL_CONFIG['password']}' "
    f"-h'{MYSQL_CONFIG['host']}'"
)

REPORTS_DIR = os.path.join(BASE_DIR, "reports")
LOGS_DIR = os.path.join(BASE_DIR, 'logs')
BASELINE_REPORT_FILENAME = "baseline_report.md"
EVENTS_REPORT_FILENAME_TEMPLATE = "events_report_{date}.md"

# Используем словарь для стабильности ключей в анализаторе
MONITOR_COMMANDS = [
    {'key': 'top', 'command': 'top -b -n 1'},
    {'key': 'free', 'command': 'free -m'},
    {'key': 'meminfo', 'command': 'cat /proc/meminfo'},
    {'key': 'cpuinfo', 'command': 'cat /proc/cpuinfo'},
    {'key': 'vmstat', 'command': 'vmstat 1 5'},
    # {'key': 'iostat', 'command': 'iostat -x 1 3'}, # Команда закомментирована, т.к. iostat не установлен
    {'key': 'processlist', 'command': f"{mysql_conn_string} -e \"SHOW FULL PROCESSLIST;\""},
    {'key': 'global_status', 'command': f"{mysql_conn_string} -e \"SHOW GLOBAL STATUS;\""},
    {'key': 'global_variables', 'command': f"{mysql_conn_string} -e \"SHOW GLOBAL VARIABLES;\""},
    {'key': 'innodb_status', 'command': f"{mysql_conn_string} -e \"SHOW ENGINE INNODB STATUS;\""},
    {'key': 'qcache_status', 'command': f"{mysql_conn_string} -e \"SHOW STATUS LIKE 'Qcache%';\""}, # Исправлен синтаксис
]

# Настройки мониторинга
HIGH_FREQ_MONITORING_ENABLED = True
HIGH_FREQ_CPU_THRESHOLD = float(os.getenv('HIGH_FREQ_CPU_THRESHOLD', 80.0))
HIGH_FREQ_MEMORY_THRESHOLD = float(os.getenv('HIGH_FREQ_MEMORY_THRESHOLD', 90.0))
HIGH_FREQ_MONITORING_INTERVAL = int(os.getenv('HIGH_FREQ_MONITORING_INTERVAL', 10))  # секунды

# Интервал для непрерывного мониторинга (в секундах)
CONTINUOUS_MONITOR_INTERVAL_SECONDS = 10

# Отладочный режим
DEBUG_MODE = os.getenv('DEBUG_MODE', 'False').lower() == 'true'  # Установите True для включения подробного логирования

# Настройки логирования
LOG_TO_FILE = os.getenv('LOG_TO_FILE', 'True').lower() == 'true'  # Записывать логи в файл
LOG_TO_CONSOLE = os.getenv('LOG_TO_CONSOLE', 'True').lower() == 'true'  # Выводить логи в консоль
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()  # Уровень логирования: DEBUG, INFO, WARNING, ERROR, CRITICAL

MEMORY_MONITOR_INTERVAL_SECONDS = int(os.getenv('MEMORY_MONITOR_INTERVAL_SECONDS', 1800))  # 30 минут

EMAIL_REPORT_TIMES = [t.strip() for t in os.getenv('EMAIL_REPORT_TIMES', '09:00,23:59').split(',') if t.strip()] 