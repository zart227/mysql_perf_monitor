import re
from core.logger import logger
from datetime import datetime

class Analyzer:
    """
    Класс-заглушка для предоставления пороговых значений.
    В будущем может быть расширен для более сложного анализа.
    """
    def __init__(self, metrics, cpu_spikes=None):
        """
        :param metrics: Словарь с собранными метриками (в текущей реализации не используется).
        :param cpu_spikes: Список зафиксированных пиков CPU (в текущей реализации не используется).
        """
        self.metrics = metrics or {}
        self.cpu_spikes = cpu_spikes or []
        self.events = {}
        self.issues = []
        self.recommendations = []
        # Пороговое значение использования памяти в процентах.
        self.memory_threshold = 90
        # Можно добавить другие пороги здесь
        # self.cpu_threshold = 80

    def analyze(self):
        """
        Запускает анализ всех метрик и возвращает структурированный результат.
        """
        return self.events

    def check_cpu_spikes(self):
        spikes = self.metrics.get('cpu_spikes', [])
        if not spikes:
            return
            
        self.issues.append(f"🔥 Обнаружено кратковременных пиков CPU: {len(spikes)}")
        for spike in spikes:
            ts = spike['timestamp']
            cpu = spike['cpu_usage']
            process_line = spike['triggering_process_line']
            processlist = spike['processlist_output']
            
            heavy_query_info = "не найден"
            max_time = -1
            
            # Сбрасываем информацию перед анализом
            spike['heavy_query_info'] = heavy_query_info 
            spike['recommendation_dba'] = "Не удалось определить проблемный запрос."
            spike['recommendation_sysadmin'] = "Пик нагрузки на CPU был вызван процессом `mysqld`. Проблема, вероятно, на стороне базы данных."
            spike['vmstat_output'] = self.metrics.get('vmstat', 'N/A')

            for line in processlist.splitlines():
                if 'Query' in line:
                    try:
                        parts = re.split(r'\s+', line.strip())
                        if len(parts) > 7 and parts[4] == 'Query':
                            time_val = int(parts[5])
                            if time_val > max_time:
                                max_time = time_val
                                query_text = ' '.join(parts[7:]).replace('`', '\\`')
                                heavy_query_info = f"время {time_val}с, запрос: `{query_text}`"
                                
                                # Формируем рекомендации
                                spike['recommendation_dba'] = f"Проанализируйте и оптимизируйте запрос, выполнявшийся {time_val}с. Проверьте наличие подходящих индексов для таблицы, к которой он обращается. Запрос: `{query_text}`"

                    except (ValueError, IndexError):
                        continue
            
            spike['heavy_query_info'] = heavy_query_info
            self.issues.append(f"  - В **{ts}** скачок CPU до **{cpu}%**. Процесс: `{process_line}`. Самый долгий запрос: {heavy_query_info}.")
        
        self.recommendations.append("Обнаружены кратковременные пики CPU. Проанализируйте запросы, которые выполнялись в моменты пиков, и оптимизируйте их.")

    def check_memory(self):
        free_output = self.metrics.get('free', '')
        if not free_output:
            return
            
        mem_match = re.search(r'Mem:\s+(\d+)\s+(\d+)', free_output)
        if mem_match:
            total = int(mem_match.group(1))
            used = int(mem_match.group(2))
            percent = used / total * 100 if total else 0
            
            if percent > 90:
                logger.warning(f"Обнаружено высокое потребление памяти: {percent:.1f}%")
                if 'memory_events' not in self.metrics:
                    self.metrics['memory_events'] = {}
                
                self.metrics['memory_events']['memory'] = [{
                    'timestamp': datetime.now().strftime('%H:%M:%S'),
                    'usage_percent': f"{percent:.1f}%",
                    'raw_output': free_output,
                    'vmstat_output': self.metrics.get('vmstat', 'N/A'),
                    'recommendation_dba': "Проверьте настройку `innodb_buffer_pool_size`. Возможно, она слишком велика для доступной оперативной памяти или требует тюнинга.",
                    'recommendation_sysadmin': f"Использование памяти составляет {percent:.1f}%. Проверьте, нет ли других процессов, потребляющих много памяти. Если основное потребление приходится на `mysqld`, скоординируйтесь с администратором БД."
                }]
                self.issues.append(f'Высокое использование памяти: {percent:.1f}%')
                self.recommendations.append('Проверьте процессы, утечки памяти, настройте параметры innodb_buffer_pool_size.')

    def check_long_queries(self):
        return

    def check_qcache(self):
        qcache = self.metrics.get('qcache_status', '')
        if qcache:
            hits_match = re.search(r'Qcache_hits\s+(\d+)', qcache)
            inserts_match = re.search(r'Qcache_inserts\s+(\d+)', qcache)
            if hits_match and inserts_match:
                hits = int(hits_match.group(1))
                inserts = int(inserts_match.group(1))
                total_queries = hits + inserts
                if total_queries > 0:
                    hit_rate = hits / total_queries
                    if hit_rate < 0.8:
                        self.issues.append(f'Низкий кэш-хит: {hit_rate:.2%}')
                        self.recommendations.append('Проверьте настройки query_cache_size и query_cache_type.') 

    def _find_heavy_query(self, processlist_output):
        """
        Находит самый "тяжелый" запрос (дольше всего выполняется)
        из вывода SHOW FULL PROCESSLIST.
        """
        if not processlist_output:
            return "Не удалось получить список процессов."
        
        lines = processlist_output.strip().split('\\n')
        if len(lines) < 2:
            return "Список процессов пуст или имеет неверный формат."

        processes = []
        header = [h.strip() for h in lines[0].split('\\t')]
        
        try:
            time_col_index = header.index('Time')
            info_col_index = header.index('Info')
        except ValueError:
            return "Не найдены колонки 'Time' или 'Info' в выводе PROCESSLIST."

        for line in lines[1:]:
            if not line.strip(): continue
            parts = [p.strip() for p in line.split('\\t')]
            if len(parts) > max(time_col_index, info_col_index):
                try:
                    time_val = int(parts[time_col_index])
                    info_val = parts[info_col_index]
                    # Исключаем спящие процессы и системные потоки
                    if info_val and info_val != 'NULL' and 'sleep' not in parts[header.index('Command')].lower():
                        processes.append({'time': time_val, 'info': info_val})
                except (ValueError, IndexError):
                    continue
        
        if not processes:
            return "Активных запросов не найдено."

        # Сортируем по времени выполнения
        longest_running = sorted(processes, key=lambda x: x['time'], reverse=True)[0]
        return longest_running['info']

    def _analyze_cpu_spikes(self):
        """Анализирует зафиксированные пики CPU."""
        if not self.cpu_spikes:
            return

        for spike in self.cpu_spikes:
            heavy_query = self._find_heavy_query(spike.get('processlist_output', ''))
            spike['heavy_query_info'] = heavy_query
            spike['recommendation_dba'] = f"Проверьте и оптимизируйте запрос, который мог вызвать нагрузку: `{heavy_query}`. Проверьте индексы для таблиц, используемых в этом запросе."
            spike['recommendation_sysadmin'] = "Нагрузка на CPU вызвана процессом mysqld. Проблема, скорее всего, на стороне базы данных. Предоставьте DBA информацию о запросе-виновнике."
        
        self.events['cpu_spikes'] = self.cpu_spikes

    def _analyze_memory(self):
        """Анализирует использование памяти."""
        meminfo_str = self.metrics.get('free', '') # Используем 'free -m' для простоты
        if not meminfo_str:
            return

        try:
            lines = meminfo_str.strip().split('\\n')
            mem_line = ""
            for line in lines:
                if line.startswith('Mem:'):
                    mem_line = line
                    break
            
            if not mem_line: return

            parts = mem_line.split()
            total = int(parts[1])
            used = int(parts[2])
            usage_percent = (used / total) * 100 if total > 0 else 0

            if usage_percent > 90:
                logger.warning(f"Обнаружено высокое потребление памяти: {usage_percent:.1f}%")
                self.events['memory_events'].append({
                    'usage_percent': f"{usage_percent:.1f}%",
                    'raw_output': self.metrics.get('free', ''),
                    'vmstat_output': self.metrics.get('vmstat', ''),
                    'recommendation_dba': "Проверьте настройку `innodb_buffer_pool_size`. Возможно, она слишком велика для доступной оперативной памяти или требует тюнинга.",
                    'recommendation_sysadmin': f"Использование памяти составляет {usage_percent:.1f}%. Проверьте, нет ли других процессов, потребляющих много памяти. Если основное потребление приходится на `mysqld`, скоординируйтесь с администратором БД."
                })
        except (ValueError, IndexError) as e:
            logger.error(f"Ошибка при анализе памяти: {e}", exc_info=True)
        
        self.events['memory_events'] = self.metrics.get('memory_events', []) 