import os
import time
from datetime import datetime
import paramiko
from paramiko.ssh_exception import AuthenticationException
import sys
import schedule
import signal
import threading

from core.ssh_client import SSHClient
from core.metrics_collector import MetricsCollector
from core.analyzer import Analyzer
from report.report_generator import generate_baseline_report, append_cpu_event_to_report, append_memory_event_to_report, check_if_memory_event_exists, generate_daily_summary_report
from core.logger import logger
from config.config import (
    SSH_HOST, SSH_PORT, SSH_USER, SSH_PASSWORD,
    HIGH_FREQ_CPU_THRESHOLD,
    REPORTS_DIR,
    BASELINE_REPORT_FILENAME,
    EVENTS_REPORT_FILENAME_TEMPLATE,
    CONTINUOUS_MONITOR_INTERVAL_SECONDS,
    EMAIL_ENABLED,
    MEMORY_MONITOR_INTERVAL_SECONDS,
    EMAIL_REPORT_TIMES,
    ENABLE_AI,
    ENABLE_PROXY,
    ARCHIVE_ENABLED,
    ARCHIVE_DAILY_TIME
)
from core.email_utils import send_report_email, build_html_report_email
from tools.archive_manager import run_archive_cleanup

print('CWD:', os.getcwd())
print('__file__:', __file__)

os.makedirs(REPORTS_DIR, exist_ok=True)

ssh_client = None  # Глобальная переменная для доступа из обработчика

def handle_exit(signum, frame):
    logger.info(f"Получен сигнал завершения ({signum}). Завершаю работу.")
    global ssh_client
    if ssh_client and ssh_client.is_connected():
        ssh_client.close()
    logger.info("Сервис мониторинга MySQL остановлен.")
    sys.exit(0)

signal.signal(signal.SIGTERM, handle_exit)
signal.signal(signal.SIGINT, handle_exit)

def continuous_monitoring(ssh_client, mysql_pid):
    """
    Функция для непрерывного мониторинга CPU и памяти.
    Добавлен heartbeat-лог и расширенная обработка ошибок.
    """
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
                    # Собираем доп. информацию в момент пика
                    process_list = metrics_collector.get_mysql_processlist()
                    performance_analysis = metrics_collector.analyze_query_performance(process_list)
                    event_report_path = os.path.join(
                        REPORTS_DIR,
                        EVENTS_REPORT_FILENAME_TEMPLATE.format(date=datetime.now().strftime('%Y%m%d'))
                    )
                    append_cpu_event_to_report(
                        {
                            'time': datetime.now().strftime('%H:%M:%S'), 
                            'cpu': cpu_usage, 
                            'pid': mysql_pid,
                            'process_list': process_list,
                            'performance_analysis': performance_analysis
                        }, 
                        event_report_path
                    )
                # 2. Мониторинг памяти (раз в MEMORY_MONITOR_INTERVAL_SECONDS)
                now = time.time()
                if now - last_memory_check >= MEMORY_MONITOR_INTERVAL_SECONDS:
                    try:
                        memory_usage = metrics_collector.get_memory_usage_percent()
                        analyzer = Analyzer({}, [])
                        memory_threshold = analyzer.memory_threshold
                        if memory_usage is not None and memory_usage > memory_threshold:
                            event_report_path = os.path.join(
                                REPORTS_DIR,
                                EVENTS_REPORT_FILENAME_TEMPLATE.format(date=datetime.now().strftime('%Y%m%d'))
                            )
                            if not check_if_memory_event_exists(event_report_path):
                                append_memory_event_to_report(
                                    {'time': datetime.now().strftime('%H:%M:%S'), 'memory_percent': memory_usage},
                                    event_report_path
                                )
                                logger.warning(f"Информация о памяти добавлена в {event_report_path}")
                    except Exception as e:
                        logger.error(f"Ошибка при мониторинге памяти: {e}", exc_info=True)
                    last_memory_check = now
                # Heartbeat лог раз в минуту
                if now - last_heartbeat >= 60:
                    logger.info(f"HEARTBEAT: сервис работает, PID: {mysql_pid}, время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    last_heartbeat = now
                # Ждем до следующей итерации CPU
                elapsed = time.time() - start_time
                sleep_time = max(0, CONTINUOUS_MONITOR_INTERVAL_SECONDS - elapsed)
                time.sleep(sleep_time)
            except Exception as e:
                logger.error(f"Ошибка в цикле мониторинга: {e}", exc_info=True)
    except KeyboardInterrupt:
        logger.info("Получен сигнал KeyboardInterrupt. Завершаю непрерывный мониторинг.")
    except Exception as e:
        logger.error(f"Критическая ошибка в continuous_monitoring: {e}", exc_info=True)

def send_daily_report():
    today = datetime.now().strftime('%Y%m%d')
    baseline_path = os.path.join(REPORTS_DIR, BASELINE_REPORT_FILENAME)
    events_path = os.path.join(REPORTS_DIR, EVENTS_REPORT_FILENAME_TEMPLATE.format(date=today))
    summary_path = os.path.join(REPORTS_DIR, f'daily_summary_{today}.md')
    if os.path.exists(baseline_path) and os.path.exists(events_path):
        generate_daily_summary_report(baseline_path, events_path, summary_path)
        try:
            body = (
                f"Во вложении — два автоматических отчёта по MySQL за {today}:\n"
                f"\n"
                f"1. events_report_{today}.md — подробный событийный отчёт (пики нагрузки, топ-5 долгих запросов, рекомендации).\n"
                f"2. daily_summary_{today}.md — краткая сводка по дню (агрегированные показатели, AI-рекомендации).\n"
                f"\nЕсли потребуется дополнительная детализация — дайте знать."
            )
            html_body = f"""
            <html>
              <body style='font-family: Arial, sans-serif; color: #222;'>
                <h2>Добрый день, Рутем!</h2>
                <p>Во вложении — <b>два автоматических отчёта</b> по производительности MySQL за <b>{today}</b>:</p>
                <ul>
                  <li><b>events_report_{today}.md</b> — подробный событийный отчёт (пики нагрузки, топ-5 долгих запросов, рекомендации).</li>
                  <li><b>daily_summary_{today}.md</b> — краткая сводка по дню (агрегированные показатели, AI-рекомендации).</li>
                </ul>
                <p>Если потребуется дополнительная детализация — дайте знать.</p>
                <p style='margin-top:20px;'>С уважением,<br>MySQL Perf Monitor<br><a href='https://github.com/zart227/mysql_perf_monitor'>Проект на GitHub</a></p>
              </body>
            </html>
            """
            send_report_email(
                subject=f"MySQL Perf Reports {today}",
                body=body,
                attachments=[events_path, summary_path],
                html_body=html_body
            )
        except Exception as e:
            logger.error(f"Ошибка при отправке email: {e}", exc_info=True)
    else:
        logger.warning(f"Файлы baseline или событийного отчёта не найдены для отправки: {baseline_path}, {events_path}")

def main():
    global ssh_client
    logger.info("Сервис мониторинга MySQL запущен в режиме непрерывного отслеживания.")
    
    # Запуск архивации и очистки при старте
    if ARCHIVE_ENABLED:
        try:
            logger.info("Запуск процедуры архивации и очистки...")
            run_archive_cleanup()
        except Exception as e:
            logger.error(f"Ошибка при архивации: {e}", exc_info=True)

    ssh_client = SSHClient()
    try:
        ssh_client.connect()
        metrics_collector = MetricsCollector(ssh_client)

        # --- Этап 1: Сбор базовых метрик (выполняется один раз) ---
        logger.info("Начинаю сбор основных метрик для базового отчета...")
        baseline_metrics_data = metrics_collector.collect_baseline_metrics()
        logger.info("Сбор основных метрик для базового отчета завершен.")

        baseline_report_path = os.path.join(REPORTS_DIR, BASELINE_REPORT_FILENAME)
        if not os.path.exists(baseline_report_path):
            logger.info(f"Создаю базовый отчет: {baseline_report_path}")
            generate_baseline_report(baseline_metrics_data, baseline_report_path)
            logger.info("Базовый отчет успешно создан.")
        else:
            logger.info(f"Базовый отчет {baseline_report_path} уже существует. Пропускаю создание.")
        
        logger.info("="*30)

        # --- Этап 2: Непрерывный мониторинг в отдельном потоке ---
        mysql_pid = metrics_collector.get_mysqld_pid()
        if not mysql_pid:
            logger.error("Не удалось получить PID процесса mysqld. Непрерывный мониторинг невозможен.")
            return

        monitor_thread = threading.Thread(target=continuous_monitoring, args=(ssh_client, mysql_pid), daemon=True)
        monitor_thread.start()

        # Планировщик email-отчётов
        if EMAIL_ENABLED:
            for t in EMAIL_REPORT_TIMES:
                schedule.every().day.at(t).do(send_daily_report)
        
        # Планировщик архивации
        if ARCHIVE_ENABLED:
            schedule.every().day.at(ARCHIVE_DAILY_TIME).do(run_archive_cleanup)
            logger.info(f"Запланирована ежедневная архивация в {ARCHIVE_DAILY_TIME}")

        # Heartbeat в основном потоке
        last_main_heartbeat = time.time()
        
        while True:
            schedule.run_pending()
            
            # Heartbeat каждые 30 секунд в основном потоке
            now = time.time()
            if now - last_main_heartbeat >= 30:
                logger.info(f"HEARTBEAT: основной поток работает, время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                last_main_heartbeat = now
                
            time.sleep(30)

    except AuthenticationException:
        print("[CRITICAL] Ошибка SSH: неверный логин или пароль. Проверьте переменные окружения в .env!")
        logger.critical("Ошибка SSH: неверный логин или пароль. Проверьте переменные окружения в .env!")
        if ssh_client:
            ssh_client.close()
        sys.exit(1)
    except Exception as e:
        logger.error(f"Критическая ошибка в приложении: {e}", exc_info=True)
        if ssh_client:
            ssh_client.close()
        print(f"[CRITICAL] Необработанная ошибка: {e}")
        sys.exit(1)
    finally:
        if ssh_client and ssh_client.is_connected():
            ssh_client.close()
        logger.info("Сервис мониторинга MySQL остановлен.")


if __name__ == '__main__':
    if '--send-report-now' in sys.argv:
        today = datetime.now().strftime('%Y%m%d')
        events_path = os.path.join(REPORTS_DIR, EVENTS_REPORT_FILENAME_TEMPLATE.format(date=today))
        summary_path = os.path.join(REPORTS_DIR, f'daily_summary_{today}.md')
        body = (
            f"Во вложении — два автоматических отчёта по MySQL за {today}:\n"
            f"\n"
            f"1. events_report_{today}.md — подробный событийный отчёт (пики нагрузки, топ-5 долгих запросов, рекомендации).\n"
            f"2. daily_summary_{today}.md — краткая сводка по дню (агрегированные показатели, AI-рекомендации).\n"
            f"\nЕсли потребуется дополнительная детализация — дайте знать."
        )
        html_body = f"""
        <html>
          <body style='font-family: Arial, sans-serif; color: #222;'>
            <h2>Добрый день, Рутем!</h2>
            <p>Во вложении — <b>два автоматических отчёта</b> по производительности MySQL за <b>{today}</b>:</p>
            <ul>
              <li><b>events_report_{today}.md</b> — подробный событийный отчёт (пики нагрузки, топ-5 долгих запросов, рекомендации).</li>
              <li><b>daily_summary_{today}.md</b> — краткая сводка по дню (агрегированные показатели, AI-рекомендации).</li>
            </ul>
            <p>Если потребуется дополнительная детализация — дайте знать.</p>
            <p style='margin-top:20px;'>С уважением,<br>MySQL Perf Monitor<br><a href='https://github.com/zart227/mysql_perf_monitor'>Проект на GitHub</a></p>
          </body>
        </html>
        """
        try:
            send_report_email(
                subject=f"MySQL Perf Reports {today}",
                body=body,
                attachments=[events_path, summary_path],
                html_body=html_body
            )
            print("Письмо отправлено успешно!")
        except Exception as e:
            print(f"[EMAIL ERROR] {e}")
        sys.exit(0)
    elif '--send-report-for' in sys.argv:
        # Пример: python main.py --send-report-for 20250623
        idx = sys.argv.index('--send-report-for')
        if len(sys.argv) > idx + 1:
            date_str = sys.argv[idx + 1]
            report_path = os.path.join(REPORTS_DIR, EVENTS_REPORT_FILENAME_TEMPLATE.format(date=date_str))
            html_body = build_html_report_email(date_str)
            try:
                send_report_email(
                    subject=f"MySQL Perf Report {date_str}",
                    body=f"Автоматический отчет о событиях MySQL за {date_str}",
                    attachment_path=report_path,
                    html_body=html_body
                )
                print(f"Письмо за {date_str} отправлено успешно!")
            except Exception as e:
                print(f"[EMAIL ERROR] {e}")
        else:
            print('Укажите дату для отправки отчёта (например, 20250623)')
        sys.exit(0)
    elif '--ai-test' in sys.argv:
        if ENABLE_AI:
            from core.ai_advisor import send_to_ai_advisor
            # Ручная отправка отчёта в AI
            report_path = None
            for i, arg in enumerate(sys.argv):
                if arg == '--report' and i + 1 < len(sys.argv):
                    report_path = sys.argv[i + 1]
            if not report_path:
                today = datetime.now().strftime('%Y%m%d')
                report_path = os.path.join(REPORTS_DIR, f'daily_summary_{today}.md')
            if not os.path.exists(report_path):
                print(f"Файл отчёта не найден: {report_path}")
                sys.exit(1)
            with open(report_path, encoding='utf-8') as f:
                prompt = f.read()
            print(f'Отправляю содержимое {report_path} в AI...')
            result = send_to_ai_advisor(prompt)
            print('Ответ AI:')
            print(result)
        else:
            print('AI отключён настройками.')
        sys.exit(0)
    elif '--generate-summary' in sys.argv:
        # Пример: python main.py --generate-summary 20250623
        idx = sys.argv.index('--generate-summary')
        if len(sys.argv) > idx + 1:
            date_str = sys.argv[idx + 1]
            events_path = os.path.join(REPORTS_DIR, f'events_report_{date_str}.md')
            baseline_path = os.path.join(REPORTS_DIR, 'baseline_report.md')
            summary_path = os.path.join(REPORTS_DIR, f'daily_summary_{date_str}.md')
            from report.report_generator import generate_daily_summary_report
            report = generate_daily_summary_report(baseline_path, events_path, summary_path)
            print(f'Сводный отчёт сохранён: {summary_path}')
        else:
            print('Укажите дату для генерации summary-отчёта (например, 20250623)')
        sys.exit(0)
    else:
        main() 