from core.ssh_client import SSHClient
from config.config import MONITOR_COMMANDS, HIGH_FREQ_MONITORING_ENABLED, DEBUG_MODE, MYSQL_CONFIG
from core.logger import logger
import sys

class MetricsCollector:
    def __init__(self, ssh_client):
        self.ssh = ssh_client
        # Детектор пиков больше не создается здесь

    def _execute_command(self, command):
        """Обертка для выполнения команды с логированием."""
        logger.info(f"Выполнение команды на удаленном сервере: '{command}'")
        try:
            result = self.ssh.exec_command(command)
            if result and 'Access denied' in result:
                print("[CRITICAL] Ошибка MySQL: неверный логин или пароль. Проверьте переменные окружения в .env!")
                logger.critical("Ошибка MySQL: неверный логин или пароль. Проверьте переменные окружения в .env!")
                if self.ssh:
                    self.ssh.close()
                sys.exit(1)
            if DEBUG_MODE:
                logger.info(f"Результат выполнения команды: {repr(result)}")
            return result
        except Exception as e:
            logger.error(f"Ошибка выполнения команды '{command}': {e}", exc_info=True)
            return None

    def collect_baseline_metrics(self):
        """Собирает метрики один раз при запуске для базового отчета."""
        results = {}
        mysql_user = MYSQL_CONFIG['user']
        mysql_password = MYSQL_CONFIG['password']
        mysql_host = MYSQL_CONFIG['host']
        
        commands = {
            'cpuinfo': 'cat /proc/cpuinfo',
            'global_variables': f"mysql -u'{mysql_user}' -p'{mysql_password}' -h'{mysql_host}' -e \"SHOW GLOBAL VARIABLES;\""
        }
        for key, command in commands.items():
            results[key] = self._execute_command(command)
        return results

    def get_cpu_usage_for_pid(self, pid):
        """Получает текущее использование CPU для заданного PID."""
        output = self._execute_command(f"top -b -n 1 -p {pid}")
        if not output:
            return None
        try:
            lines = output.strip().splitlines()
            for line in lines:
                if line.strip().startswith(str(pid)):
                    parts = line.split()
                    return float(parts[8].replace(',', '.'))
        except (IndexError, ValueError) as e:
            logger.error(f"Не удалось распарсить вывод top для PID {pid}: {e}\\nВывод: {output}")
        return None

    def get_memory_usage_percent(self):
        """Получает процент использования оперативной памяти."""
        output = self._execute_command("free -m")
        if not output:
            return None
        try:
            lines = output.strip().splitlines()
            if len(lines) > 1:
                parts = lines[1].split()
                total = int(parts[1])
                used = int(parts[2])
                return round((used / total) * 100, 2)
        except (IndexError, ValueError) as e:
            logger.error(f"Не удалось распарсить вывод free -m: {e}\\nВывод: {output}")
        return None

    def get_mysqld_pid(self):
        """Получает PID процесса mysqld."""
        output = self._execute_command("pidof mysqld")
        if output and output.strip():
            return output.strip().split(' ')[0]
        logger.error("Не удалось получить PID процесса mysqld.")
        return None

    def get_mysql_processlist(self):
        """Получает топ-5 самых долгих запросов, исключая спящие, собственную сессию и системных демонов."""
        mysql_user = MYSQL_CONFIG['user']
        mysql_password = MYSQL_CONFIG['password']
        mysql_host = MYSQL_CONFIG['host']
        
        command = f"mysql -u'{mysql_user}' -p'{mysql_password}' -h'{mysql_host}' -e \"SELECT ID, USER, HOST, DB, COMMAND, TIME, STATE, INFO FROM information_schema.PROCESSLIST WHERE COMMAND != 'Sleep' AND ID != CONNECTION_ID() AND USER != 'event_scheduler' ORDER BY TIME DESC LIMIT 5\" --table"
        result = self._execute_command(command)
        if DEBUG_MODE:
            logger.info(f"Результат MySQL processlist: {repr(result)}")
        return result

    def analyze_query_performance(self, process_list):
        """Анализирует производительность запросов из processlist."""
        if not process_list or not process_list.strip():
            return None
            
        try:
            lines = process_list.strip().splitlines()
            if not lines or lines[0].startswith('+'):
                # Парсим стандартный вывод MySQL
                header_line = None
                data_lines = []
                
                for line in lines:
                    if line.startswith('|') and not line.startswith('+-'):
                        if header_line is None:
                            header_line = line
                        else:
                            data_lines.append(line)
                
                if header_line and data_lines:
                    headers = [h.strip() for h in header_line.split('|')[1:-1]]
                    queries = []
                    
                    for data_line in data_lines:
                        row = [cell.strip() for cell in data_line.split('|')[1:-1]]
                        if len(row) == len(headers):
                            query_data = dict(zip(headers, row))
                            try:
                                query_data['TIME'] = int(query_data.get('TIME', 0))
                            except (ValueError, TypeError):
                                query_data['TIME'] = 0
                            queries.append(query_data)
                    
                    # Анализируем время выполнения
                    if queries:
                        analysis = {
                            'total_queries': len(queries),
                            'max_time': max(q['TIME'] for q in queries),
                            'avg_time': sum(q['TIME'] for q in queries) / len(queries),
                            'slow_queries': [q for q in queries if q['TIME'] > 10],  # Запросы дольше 10 секунд
                            'critical_queries': [q for q in queries if q['TIME'] > 30],  # Запросы дольше 30 секунд
                            'queries_by_time': sorted(queries, key=lambda x: x['TIME'], reverse=True)
                        }
                        return analysis
            
            return None
        except Exception as e:
            logger.error(f"Ошибка анализа производительности запросов: {e}")
            return None

    def get_qcache_status(self):
        """Получает статус QCache."""
        return self.ssh.exec_command(f"{mysql_conn_string} -e \"SHOW STATUS LIKE 'Qcache%';\"") 