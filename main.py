import threading
import time
import os
from datetime import datetime
import schedule
import sys
import signal

from core.ssh_client import SSHClient
from core.metrics_collector import MetricsCollector
from report.report_generator import (
    generate_baseline_report, append_cpu_event_to_report,
    append_memory_event_to_report, check_if_memory_event_exists,
    generate_daily_summary_report
)
from core.logger import logger
from config.config import (
    SSH_HOST, SSH_PORT, SSH_USER, SSH_PASSWORD,
    REPORTS_DIR, BASELINE_REPORT_FILENAME, EVENTS_REPORT_FILENAME_TEMPLATE,
    HIGH_FREQ_CPU_THRESHOLD, HIGH_FREQ_MEMORY_THRESHOLD,
    CONTINUOUS_MONITOR_INTERVAL_SECONDS, MEMORY_MONITOR_INTERVAL_SECONDS,
    EMAIL_ENABLED, EMAIL_REPORT_TIMES, SSH_CONFIG
)
from core.email_utils import send_report_email

# Настройка логирования
ssh_client_global = None

def handle_exit(signum, frame):
    logger.info("Получен сигнал завершения. Закрываю SSH-соединение...")
    global ssh_client_global
    if ssh_client_global:
        ssh_client_global.close()
    logger.info("Приложение завершает работу.")
    sys.exit(0)

signal.signal(signal.SIGTERM, handle_exit)
signal.signal(signal.SIGINT, handle_exit)

def high_frequency_monitoring(ssh_client, mysql_pid):
    """Функция для непрерывного мониторинга CPU и памяти."""
    try:
        logger.info(f"Запуск непрерывного мониторинга для PID: {mysql_pid} с интервалом {CONTINUOUS_MONITOR_INTERVAL_SECONDS} сек.")
        metrics_collector = MetricsCollector(ssh_client)
        last_memory_check = 0
        last_heartbeat = 0

        while True:
            try:
                start_time = time.time()
                
                # 1. Мониторинг CPU (часто)
                cpu_usage = metrics_collector.get_cpu_usage_for_pid(mysql_pid)
                if cpu_usage is not None and cpu_usage > HIGH_FREQ_CPU_THRESHOLD:
                    logger.warning(f"Обнаружен всплеск CPU: {cpu_usage}%")
                    process_list = metrics_collector.get_mysql_processlist()
                    
                    # Данные для отчета
                    event_data = {
                        'time': datetime.now().strftime('%H:%M:%S'),
                        'cpu': cpu_usage,
                        'pid': mysql_pid,
                        'process_list': process_list
                    }
                    
                    event_report_path = os.path.join(
                        REPORTS_DIR,
                        EVENTS_REPORT_FILENAME_TEMPLATE.format(date=datetime.now().strftime('%Y%m%d'))
                    )
                    append_cpu_event_to_report(event_data, event_report_path)

                # 2. Мониторинг памяти (реже)
                now = time.time()
                if now - last_memory_check >= MEMORY_MONITOR_INTERVAL_SECONDS:
                    try:
                        memory_usage = metrics_collector.get_memory_usage_percent()
                        if memory_usage is not None and memory_usage > HIGH_FREQ_MEMORY_THRESHOLD:
                            event_report_path = os.path.join(REPORTS_DIR, EVENTS_REPORT_FILENAME_TEMPLATE.format(date=datetime.now().strftime('%Y%m%d')))
                            if not check_if_memory_event_exists(event_report_path):
                                append_memory_event_to_report(
                                    {'time': datetime.now().strftime('%H:%M:%S'), 'memory_percent': memory_usage},
                                    event_report_path
                                )
                    except Exception as e:
                        logger.error(f"Ошибка при мониторинге памяти: {e}", exc_info=True)
                    last_memory_check = now

                # Heartbeat лог раз в минуту
                if now - last_heartbeat >= 60:
                    logger.info(f"HEARTBEAT: сервис работает, PID: {mysql_pid}, время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    last_heartbeat = now
                
                elapsed = time.time() - start_time
                sleep_time = max(0, CONTINUOUS_MONITOR_INTERVAL_SECONDS - elapsed)
                time.sleep(sleep_time)

            except Exception as e:
                logger.error(f"Ошибка в цикле мониторинга: {e}", exc_info=True)
                time.sleep(60)

    except Exception as e:
        logger.error(f"Критическая ошибка в high_frequency_monitoring: {e}", exc_info=True)

def main():
    logger.info("Сервис мониторинга MySQL запущен.")
    global ssh_client_global
    
    try:
        ssh_client_global = SSHClient()
        ssh_client_global.connect()
        logger.info(f"SSH подключение к {SSH_CONFIG['host']} успешно установлено.")

        mysql_pid = MetricsCollector(ssh_client_global).get_mysqld_pid()
        if not mysql_pid:
            logger.critical("Не удалось получить PID процесса mysqld.")
            return

        baseline_report_path = os.path.join(REPORTS_DIR, BASELINE_REPORT_FILENAME)
        if not os.path.exists(baseline_report_path):
            logger.info("Сбор метрик для базового отчета...")
            baseline_metrics = MetricsCollector(ssh_client_global).collect_baseline_metrics()
            generate_baseline_report(baseline_metrics, baseline_report_path)
        else:
            logger.info(f"Базовый отчет {baseline_report_path} уже существует.")

        monitor_thread = threading.Thread(target=high_frequency_monitoring, args=(ssh_client_global, mysql_pid), daemon=True)
        monitor_thread.start()

        if EMAIL_ENABLED:
            for report_time in EMAIL_REPORT_TIMES:
                schedule.every().day.at(report_time).do(send_report_email, report_date=datetime.now().strftime('%Y%m%d'))

        while True:
            schedule.run_pending()
            time.sleep(1)

    except Exception as e:
        logger.critical(f"Критическая ошибка в main: {e}", exc_info=True)
    finally:
        if ssh_client_global:
            ssh_client_global.close()
        logger.info("Приложение остановлено.")

if __name__ == '__main__':
    main() 