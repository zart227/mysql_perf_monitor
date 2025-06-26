from jinja2 import Template
from datetime import datetime
import os
import pandas as pd
import io
import re
import logging
from report.ai_prompt_utils import build_ai_prompt
from core.ai_advisor import send_to_ai_advisor
import collections
from config.config import ENABLE_AI
import csv

logger = logging.getLogger(__name__)

REPORT_TEMPLATE = """
# Отчёт по производительности MySQL

**Дата:** {{ date }}

## Найденные проблемы и рекомендации

{% if issues %}
### Проблемы:
{% for issue in issues %}
- {{ issue }}
{% endfor %}
{% else %}
Критичных проблем не обнаружено.
{% endif %}

{% if recommendations %}
### Рекомендации:
{% for rec in recommendations %}
- {{ rec }}
{% endfor %}
{% endif %}

---

## Детальные метрики

{% for key, output in metrics.items() %}
{% if key != 'cpu_spikes' %}
### {{ key }}
{% if key in ['global_status', 'global_variables', 'qcache_status', 'processlist'] and '---' in output %}
{{ output }}
{% else %}
```
{{ output }}
```
{% endif %}
{% endif %}
{% endfor %}

{% if metrics.cpu_spikes %}
## Информация о пиках CPU

{% for spike in metrics.cpu_spikes %}
### Пик в {{ spike.timestamp }} (CPU: {{ spike.cpu_usage }}%)

**Процесс-виновник:**
```
{{ spike.triggering_process_line }}
```

**Список запросов в момент пика (`SHOW FULL PROCESSLIST`):**
{{ spike.processlist_output }}
---
{% endfor %}
{% endif %}
"""

BASELINE_TEMPLATE = """
# Базовый отчет о конфигурации MySQL
**Дата создания:** {{ date }}
---
## Информация о CPU
{{ metrics.cpuinfo }}
---
## Информация о памяти
{{ metrics.memory }}
---
## Глобальные переменные MySQL
{{ metrics.global_variables }}
"""

EVENT_HEADER_TEMPLATE = """
# Журнал событий производительности за {{ date }}
"""

CPU_EVENT_TEMPLATE = """
---
### 📈 Пик CPU в {{ time }}
- **PID процесса:** `{{ pid }}`
- **Зафиксированная нагрузка:** `{{ cpu_percent }}%`

**Топ-5 запросов по времени выполнения в момент пика:**
{{ process_list }}
"""

MEMORY_EVENT_TEMPLATE = """
---
### 📉 Высокое потребление памяти в {{ time }}
- **Зафиксированное использование:** `{{ memory_percent }}%`
"""

def parse_innodb_status(status_string):
    """
    Парсит вывод SHOW ENGINE INNODB STATUS, который может быть в двух форматах:
    1. Табличный (с \t и \n)
    2. Вертикальный (с \G)
    """
    if "***************************" in status_string:
        # Вертикальный формат (\G)
        match = re.search(r'Status:\n(.*?)$', status_string, re.DOTALL)
        if match:
            return match.group(1).strip()
    else:
        # Табличный формат
        parts = status_string.split('\t')
        if len(parts) > 2:
            return parts[2].replace('\\n', '\n').strip()
    
    # Fallback
    return status_string.replace('\\n', '\n').strip()

def to_markdown_table(data):
    """Преобразует табличные данные (строка с табуляцией или markdown) в markdown-таблицу."""
    if not data or not isinstance(data, str):
        return data or ''
    # Если есть табуляции, пробуем через pandas
    if '\t' in data:
        try:
            df = pd.read_csv(io.StringIO(data), sep='\t', engine='python')
            return df.to_markdown(index=False)
        except Exception as e:
            return f"```\n(ошибка парсинга таблицы: {e})\n{data}\n```"
    return data

def parse_and_format_free_output(free_output):
    """Парсит вывод 'free -m' и форматирует его в виде markdown-таблицы и таблицы buffers/cache."""
    if not free_output or not isinstance(free_output, str):
        return f"```\n{free_output or 'N/A'}\n```"
    try:
        lines = free_output.strip().splitlines()
        # Основная таблица памяти
        main_table = '\n'.join(lines[:3])
        main_table_md = to_markdown_table(main_table)
        # Таблица для buffers/cache
        buffer_line = lines[2]
        buffer_parts = buffer_line.split()
        buffer_used = buffer_parts[2]
        buffer_free = buffer_parts[3]
        buffer_df = pd.DataFrame([
            {"Показатель": "Used (-buffers/cache)", "Значение (MB)": buffer_used},
            {"Показатель": "Free (+buffers/cache)", "Значение (MB)": buffer_free}
        ])
        table2 = buffer_df.to_markdown(index=False)
        return f"{main_table_md}\n\n**Расшифровка `-/+ buffers/cache`:**\n{table2}"
    except Exception as e:
        return f"```\n(ошибка парсинга 'free -m': {e})\n{free_output}\n```"

def parse_and_format_cpuinfo(cpuinfo_output):
    """Парсит вывод /proc/cpuinfo и форматирует в таблицу "Параметр-Значение"."""
    if not cpuinfo_output or not isinstance(cpuinfo_output, str):
        return f"```\n{cpuinfo_output or 'N/A'}\n```"
    try:
        # --- Блок для выделения информации только по первому процессору ---
        processor_blocks = cpuinfo_output.strip().split('\n\n')
        first_block = ""
        for block in processor_blocks:
            if block.strip():
                first_block = block
                break
        
        if not first_block.strip():
            processor_lines = cpuinfo_output.strip().split('\n')
            first_block_lines = []
            for line in processor_lines:
                if not line.strip() and first_block_lines:
                    break
                first_block_lines.append(line)
            first_block = "\n".join(first_block_lines)

        if not first_block.strip():
             return f"```\n(не удалось найти блок процессора в cpuinfo)\n{cpuinfo_output}\n```"
        # --- Конец блока ---

        params = []
        values = []
        for line in first_block.split('\n'):
            if ':' in line:
                parts = line.split(':', 1)
                key = parts[0].strip()
                value = parts[1].strip()
                params.append(key)
                values.append(value)

        if not params:
            return f"```\n(не удалось распознать cpuinfo)\n{cpuinfo_output}\n```"

        df = pd.DataFrame({
            'Параметр': params,
            'Значение': values
        })
        
        return df.to_markdown(index=False)
    except Exception as e:
        return f"```\n(ошибка парсинга cpuinfo: {e})\n{cpuinfo_output}\n```"

def generate_report(metrics, issues, recommendations, output_path=None):
    processed_metrics = metrics.copy()
    
    table_alignments = {
        'global_status': ("left", "left"),
        'global_variables': ("left", "left"),
        'qcache_status': ("left", "right"),
        'processlist': ("right", "left", "left", "center", "left", "right", "center", "left")
    }
    
    table_keys = list(table_alignments.keys())
    
    for key, value in processed_metrics.items():
        if not value or not isinstance(value, str):
            continue

        if key in table_keys and '\t' in value:
            try:
                df = pd.read_csv(io.StringIO(value), sep='\\t', engine='python')
                colalign = table_alignments.get(key)
                if colalign and len(df.columns) != len(colalign):
                    colalign = None # Fallback to default if column count mismatches
                processed_metrics[key] = df.to_markdown(index=False, colalign=colalign)
            except Exception:
                processed_metrics[key] = f"```\n{value}\n```"

        elif key == 'innodb_status':
            processed_metrics[key] = parse_innodb_status(value)

    if 'cpu_spikes' in processed_metrics:
        for spike in processed_metrics.get('cpu_spikes', []):
            proc_list = spike.get('processlist_output')
            if proc_list and isinstance(proc_list, str) and '\t' in proc_list:
                try:
                    df = pd.read_csv(io.StringIO(proc_list), sep='\\t', engine='python')
                    colalign = table_alignments.get('processlist')
                    if colalign and len(df.columns) != len(colalign):
                        colalign = None # Fallback to default
                    spike['processlist_output'] = df.to_markdown(index=False, colalign=colalign)
                except Exception:
                    spike['processlist_output'] = f"```\n{proc_list}\n```"

    template = Template(REPORT_TEMPLATE)
    report = template.render(
        date=datetime.now().strftime('%Y-%m-%d %H:%M'),
        metrics=processed_metrics,
        issues=issues,
        recommendations=recommendations
    )
    if output_path:
        abs_path = os.path.join(os.getcwd(), output_path) if not os.path.isabs(output_path) else output_path
        os.makedirs(os.path.dirname(abs_path), exist_ok=True)
        with open(abs_path, 'w', encoding='utf-8') as f:
            f.write(report)
    return report 

def generate_baseline_report(metrics, output_path):
    """Генерирует только базовый отчет с cpuinfo, memory и global_variables."""
    processed_metrics = {
        'cpuinfo': parse_and_format_cpuinfo(metrics.get('cpuinfo', 'N/A')),
        'memory': parse_and_format_free_output(metrics.get('memory', 'N/A')),
        'global_variables': to_markdown_table(metrics.get('global_variables'))
    }

    template = Template(BASELINE_TEMPLATE)
    report = template.render(
        date=datetime.now().strftime('%Y-%m-%d %H:%M'),
        metrics=processed_metrics
    )
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(report)

def _ensure_header(report_path):
    """Проверяет, существует ли файл и заголовок, и добавляет их при необходимости."""
    date_str = datetime.now().strftime('%Y-%m-%d')
    header = Template(EVENT_HEADER_TEMPLATE).render(date=date_str)
    
    if not os.path.exists(report_path):
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(header)
            f.write('\n')

def append_cpu_event_to_report(event_data, report_path):
    """
    Добавляет информацию о пике CPU в отчет о событиях (markdown, как раньше) и в CSV (плоский формат: одна строка на каждый запрос, info без переносов строк).
    """
    import re
    try:
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        csv_path = os.path.join(os.path.dirname(report_path), 'events_cpu.csv')
        csv_exists = os.path.exists(csv_path)
        process_list = event_data.get('process_list', '')
        # --- Парсим process_list для CSV ---
        queries = []
        if process_list and process_list.strip():
            lines = process_list.strip().splitlines()
            header_line = None
            for line in lines:
                if line.startswith('|') and 'INFO' in line.upper():
                    header_line = [h.strip().upper() for h in line.split('|')[1:-1]]
                    break
            
            if header_line:
                try:
                    # Находим индексы нужных столбцов
                    user_idx = header_line.index('USER')
                    host_idx = header_line.index('HOST')
                    time_idx = header_line.index('TIME')
                    info_idx = header_line.index('INFO')

                    for line in lines:
                        if line.startswith('|') and not line.startswith('+-') and 'USER' not in line.upper():
                            parts = [p.strip() for p in line.split('|')[1:-1]]
                            if len(parts) > max(user_idx, host_idx, time_idx, info_idx):
                                user = parts[user_idx]
                                host = parts[host_idx]
                                time_val = parts[time_idx]
                                info = parts[info_idx].replace('\n', ' ').replace('\r', ' ')
                                info = re.sub(r'\s+', ' ', info)
                                if info and info != 'NULL':
                                    queries.append({'user': user, 'host': host, 'time_query': time_val, 'info': info})
                except ValueError:
                    logger.warning("Не удалось найти все необходимые столбцы (USER, HOST, TIME, INFO) в выводе processlist.")
                except Exception as e:
                    logger.error(f"Ошибка при парсинге processlist: {e}", exc_info=True)


        # --- Запись в CSV ---
        if queries:
            with open(csv_path, 'a', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['date', 'time', 'pid', 'cpu', 'user', 'host', 'time_query', 'info']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
                if not csv_exists:
                    writer.writeheader()
                for q in queries:
                    writer.writerow({
                        'date': datetime.now().strftime('%Y-%m-%d'),
                        'time': event_data['time'],
                        'pid': event_data['pid'],
                        'cpu': event_data['cpu'],
                        'user': q['user'],
                        'host': q['host'],
                        'time_query': q['time_query'],
                        'info': q['info'],
                    })
        # --- Markdown-отчёт (как раньше) ---
        if not os.path.exists(report_path):
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write("# 📊 Отчет о событиях мониторинга MySQL\n\n")
        time_str = event_data['time']
        cpu_usage = event_data['cpu']
        pid = event_data['pid']
        # Если process_list — таблица, вставляем её как есть, иначе пишем 'Нет активных запросов.'
        if process_list and process_list.strip().startswith('+'):
            processlist_md = f'''```
{process_list.strip()}
```'''
        elif queries:
            # fallback: если таблица не распознана, но есть распарсенные запросы
            processlist_md = '| user | host | time | info |\n|---|---|---|---|\n' + '\n'.join(
                f"| {q['user']} | {q['host']} | {q['time_query']} | {q['info'][:100]}... |" for q in queries
            )
        else:
            processlist_md = 'Нет активных запросов.'
        event_entry = f"""
---
### 📈 Пик CPU в {time_str}
- **PID процесса:** `{pid}`
- **Зафиксированная нагрузка:** `{cpu_usage}%`

**Топ-5 запросов по времени выполнения в момент пика:**
{processlist_md}

"""
        performance_analysis = event_data.get('performance_analysis')
        if performance_analysis:
            event_entry += f"""
**📊 Анализ производительности запросов:**
- **Всего активных запросов:** {performance_analysis['total_queries']}
- **Максимальное время выполнения:** {performance_analysis['max_time']} сек
- **Среднее время выполнения:** {performance_analysis['avg_time']:.1f} сек
- **Медленных запросов (>10 сек):** {len(performance_analysis['slow_queries'])}
- **Критически медленных запросов (>30 сек):** {len(performance_analysis['critical_queries'])}

"""
            if performance_analysis['critical_queries']:
                event_entry += "**🚨 Критически медленные запросы (>30 сек):**\n"
                for query in performance_analysis['critical_queries']:
                    info = str(query.get('INFO', 'N/A')).replace('\n', ' ').replace('\r', ' ')
                    info = re.sub(r'\s+', ' ', info)
                    event_entry += f"- **{query['TIME']} сек:** {info[:100]}...\n"
                event_entry += "\n"
            elif performance_analysis['slow_queries']:
                event_entry += "**⚠️ Медленные запросы (>10 сек):**\n"
                for query in performance_analysis['slow_queries']:
                    info = str(query.get('INFO', 'N/A')).replace('\n', ' ').replace('\r', ' ')
                    info = re.sub(r'\s+', ' ', info)
                    event_entry += f"- **{query['TIME']} сек:** {info[:100]}...\n"
                event_entry += "\n"
        with open(report_path, 'a', encoding='utf-8') as f:
            f.write(event_entry)
        logger.info(f"Информация о пике CPU добавлена в отчет: {report_path}")
    except Exception as e:
        logger.error(f"Ошибка при добавлении информации о пике CPU в отчет: {e}", exc_info=True)

def append_memory_event_to_report(event_data, output_path):
    """Добавляет в отчет событие о высоком потреблении памяти и в CSV."""
    _ensure_header(output_path)
    # CSV-файл для памяти
    csv_path = os.path.join(os.path.dirname(output_path), 'events_memory.csv')
    csv_exists = os.path.exists(csv_path)
    with open(csv_path, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
        if not csv_exists:
            writer.writerow(['date', 'time', 'memory_percent'])
        writer.writerow([
            datetime.now().strftime('%Y-%m-%d'),
            event_data['time'],
            event_data['memory_percent']
        ])
    template = Template(MEMORY_EVENT_TEMPLATE)
    report_content = template.render(
        time=event_data['time'],
        memory_percent=event_data['memory_percent']
    )
    with open(output_path, 'a', encoding='utf-8') as f:
        f.write(report_content)

def check_if_memory_event_exists(report_path):
    """Проверяет, было ли уже сегодня событие по памяти."""
    if not os.path.exists(report_path):
        return False
    with open(report_path, 'r', encoding='utf-8') as f:
        content = f.read()
    return 'Высокое потребление памяти' in content 

def parse_and_aggregate_events(events_path):
    """
    Парсит events_report_YYYYMMDD.md и агрегирует:
    - загрузку CPU (макс/мин/среднее)
    - все запросы (группирует похожие по INFO)
    - медленные/критические запросы (медленные >1 сек)
    - статистику по времени выполнения
    """
    if not os.path.exists(events_path):
        logger.warning(f"Файл событий не найден: {events_path}")
        return {}
    
    with open(events_path, encoding='utf-8') as f:
        text = f.read()
    
    logger.info(f"Парсинг файла событий: {events_path}, размер: {len(text)} символов")
    
    # Парсим пики CPU
    cpu_usages = []
    all_queries = []
    slow_queries = []
    critical_queries = []
    query_times = []
    query_groups = collections.defaultdict(list)
    
    # Ищем все пики CPU по заголовкам ### 📈 Пик CPU
    cpu_peaks = re.findall(r'### 📈 Пик CPU в (\d{2}:\d{2}:\d{2})[\s\S]*?Зафиксированная нагрузка:\s*`([\d\.]+)%`', text)
    logger.info(f"Найдено пиков CPU по заголовкам: {len(cpu_peaks)}")
    
    for time_str, cpu_usage in cpu_peaks:
        cpu_usage = float(cpu_usage)
        cpu_usages.append(cpu_usage)
        logger.info(f"Найден пик CPU в {time_str}: {cpu_usage}%")
    
    # Ищем таблицы запросов
    table_matches = re.findall(r'\|\s*ID\s*\|.*?\n((?:\|.*?\n)+)', text, re.DOTALL)
    logger.info(f"Найдено таблиц запросов: {len(table_matches)}")
    
    for i, table in enumerate(table_matches):
        logger.info(f"Обрабатываю таблицу {i+1}")
        # Парсим строки таблицы
        for line in table.strip().split('\n'):
            if not line.strip().startswith('|'):
                continue
            parts = [p.strip() for p in line.strip('|').split('|')]
            if len(parts) < 7:
                continue
            try:
                q_id, user, host, db, command, time_val, state, info = parts[:8]
                time_val = int(time_val)
                query = {
                    'ID': q_id,
                    'USER': user,
                    'HOST': host,
                    'DB': db,
                    'COMMAND': command,
                    'TIME': time_val,
                    'STATE': state,
                    'INFO': info
                }
                all_queries.append(query)
                query_times.append(time_val)
                # Группируем по INFO (обрезаем до 100 символов для группировки)
                group_key = info[:100]
                query_groups[group_key].append(query)
                if time_val > 30:
                    critical_queries.append(query)
                elif time_val > 1:
                    slow_queries.append(query)
            except Exception as e:
                logger.debug(f"Ошибка парсинга строки таблицы: {e}")
                continue
    
    logger.info(f"Найдено пиков CPU: {len(cpu_usages)}")
    logger.info(f"Найдено запросов: {len(all_queries)}")
    logger.info(f"Медленных запросов: {len(slow_queries)}")
    logger.info(f"Критических запросов: {len(critical_queries)}")
    
    # Агрегаты
    cpu_agg = {
        'max': max(cpu_usages) if cpu_usages else None,
        'min': min(cpu_usages) if cpu_usages else None,
        'avg': sum(cpu_usages)/len(cpu_usages) if cpu_usages else None,
        'count': len(cpu_usages)
    }
    query_time_agg = {
        'max': max(query_times) if query_times else None,
        'min': min(query_times) if query_times else None,
        'avg': sum(query_times)/len(query_times) if query_times else None,
        'count': len(query_times)
    }
    # Группировка похожих запросов
    grouped_queries = []
    for key, group in query_groups.items():
        grouped_queries.append({
            'INFO': key,
            'count': len(group),
            'avg_time': sum(q['TIME'] for q in group)/len(group),
            'max_time': max(q['TIME'] for q in group),
            'min_time': min(q['TIME'] for q in group)
        })
    # Сортируем по количеству
    grouped_queries = sorted(grouped_queries, key=lambda x: x['count'], reverse=True)
    return {
        'cpu_agg': cpu_agg,
        'query_time_agg': query_time_agg,
        'grouped_queries': grouped_queries,
        'slow_queries': slow_queries,
        'critical_queries': critical_queries
    }

def generate_daily_summary_report(baseline_path, events_path, output_path):
    """
    Генерирует итоговый дневной отчёт с AI-рекомендациями и агрегированной сводкой.
    Теперь всегда использует events_cpu.csv (плоский формат) для CPU и запросов.
    """
    import pandas as pd
    today = datetime.now().strftime('%Y-%m-%d')
    date_str = today
    # Формируем промпт для AI
    prompt = build_ai_prompt(baseline_path, events_path)
    if ENABLE_AI:
        try:
            ai_recommendations = send_to_ai_advisor(prompt)
        except Exception as e:
            ai_recommendations = f"Ошибка при обращении к AI: {e}"
    else:
        ai_recommendations = 'AI отключён настройками.'
    # --- Новый блок: читаем только events_cpu.csv ---
    cpu_csv = os.path.join(os.path.dirname(events_path), 'events_cpu.csv')
    mem_csv = os.path.join(os.path.dirname(events_path), 'events_memory.csv')
    cpu_summary = ''
    mem_summary = ''
    if os.path.exists(cpu_csv):
        df = pd.read_csv(cpu_csv)
        df = df[df['date'] == date_str]
        if not df.empty:
            cpu_summary = (
                f"**CPU:**\n"
                f"  - Количество запросов: {len(df)}\n"
                f"  - Среднее значение CPU: {df['cpu'].mean():.1f}%\n"
                f"  - Максимум: {df['cpu'].max()}%\n"
                f"  - Минимум: {df['cpu'].min()}%\n"
            )
            # Статистика по времени выполнения запросов
            df['time_query'] = pd.to_numeric(df['time_query'], errors='coerce').fillna(0)
            query_time_agg = (
                f"**Время выполнения запросов:**\n"
                f"  - Среднее: {df['time_query'].mean():.1f} сек\n"
                f"  - Максимум: {df['time_query'].max()} сек\n"
                f"  - Минимум: {df['time_query'].min()} сек\n"
            )
            # Топ-5 долгих запросов
            top_long = df.sort_values('time_query', ascending=False).head(5)
            top_long_str = '\n'.join([
                f"  - {row['user']}@{row['host']} ({row['time_query']} сек): {str(row['info'])[:100]}..." for _, row in top_long.iterrows()
            ])
            # Топ-5 частых запросов (по info) с средней загрузкой CPU
            top_freq_df = df.groupby('info').agg(
                count=('info', 'size'),
                avg_cpu=('cpu', 'mean')
            ).sort_values('count', ascending=False).head(5)

            top_freq_str = '\n'.join([
                f"  - {info[:100]}... (всего: {row['count']}, ср. CPU: {row['avg_cpu']:.1f}%)" 
                for info, row in top_freq_df.iterrows()
            ])

            cpu_summary += f"\n{query_time_agg}\n**Топ-5 долгих запросов:**\n{top_long_str}\n\n**Топ-5 частых запросов:**\n{top_freq_str}\n"
    if os.path.exists(mem_csv):
        dfm = pd.read_csv(mem_csv)
        dfm = dfm[dfm['date'] == date_str]
        if not dfm.empty:
            mem_summary = (
                f"**Память:**\n"
                f"  - Количество событий: {len(dfm)}\n"
                f"  - Среднее значение: {dfm['memory_percent'].mean():.1f}%\n"
                f"  - Максимум: {dfm['memory_percent'].max()}%\n"
                f"  - Минимум: {dfm['memory_percent'].min()}%\n"
            )
    summary_str = cpu_summary + ('\n' if cpu_summary and mem_summary else '') + mem_summary
    # Формируем baseline-параметры (только ключевые, без полного baseline)
    key_params = prompt.split('Вот сводка событий за сегодня:')[0].replace('Ты — опытный администратор MySQL. Вот ключевые параметры сервера:', '').strip()
    # Итоговый markdown-отчёт
    report = f"""
# Сводный отчёт за {date_str}

## Ключевые параметры MySQL
{key_params}

## Итоговая сводка за день
{summary_str}

## AI-рекомендации (сгенерировано нейросетью)
{ai_recommendations}
"""
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(report)
    return report 