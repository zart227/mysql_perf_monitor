import re
import os
import sys
from datetime import datetime

# Добавляем корневую директорию проекта в sys.path
# чтобы можно было импортировать модули из core, report и т.д.
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.insert(0, project_root)

from report.report_generator import append_cpu_event_to_report
from core.logger import setup_logging

# Настраиваем логирование, чтобы видеть вывод в консоли
logger = setup_logging(log_to_file=False)

def test_parser_from_log(log_file_path):
    """
    Читает лог-файл, находит записи о пиках CPU и пытается
    распарсить processlist, используя основную функцию из report_generator.
    """
    logger.info(f"Читаю лог-файл: {log_file_path}")
    
    # Регулярное выражение для поиска "сырого" вывода processlist из логов
    PROCESSLIST_RE = re.compile(r"Результат MySQL processlist: (.*?)$", re.MULTILINE)
    
    with open(log_file_path, 'r', encoding='utf-8') as f:
        log_content = f.read()
        
    matches = PROCESSLIST_RE.findall(log_content)
    
    if not matches:
        logger.warning("Не найдено записей 'Результат MySQL processlist' в логе.")
        return

    logger.info(f"Найдено {len(matches)} записей processlist. Тестирую парсер на первой из них...")
    
    # Берем первую найденную запись
    # Строка приходит с repr(), поэтому убираем лишние кавычки и escape-последовательности
    raw_process_list = matches[0].strip("'\"")
    process_list = raw_process_list.encode().decode('unicode_escape')

    # Создаем тестовые данные для функции
    event_data = {
        'time': datetime.now().strftime('%H:%M:%S'),
        'cpu': 150.0,
        'pid': 99999,
        'process_list': process_list
    }

    # Путь для тестового отчета
    test_report_path = os.path.join(project_root, 'reports', 'test_events_report.md')
    
    logger.info("Вызываю append_cpu_event_to_report с тестовыми данными...")
    
    append_cpu_event_to_report(event_data, test_report_path)
    
    logger.info(f"Тестирование завершено. Проверьте файлы 'reports/test_events_report.md' и 'reports/events_cpu.csv'.")


if __name__ == '__main__':
    log_path = os.path.join(project_root, 'logs', 'mysql_perf_reporter.log')
    if not os.path.exists(log_path):
        logger.error(f"Файл логов не найден: {log_path}")
    else:
        test_parser_from_log(log_path) 