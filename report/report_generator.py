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
    """Добавляет информацию о пике CPU в отчет о событиях."""
    try:
        # Создаем директорию, если её нет
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        
        # Проверяем, существует ли файл
        if not os.path.exists(report_path):
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write("# 📊 Отчет о событиях мониторинга MySQL\n\n")
        
        # Форматируем данные события
        time_str = event_data['time']
        cpu_usage = event_data['cpu']
        pid = event_data['pid']
        process_list = event_data.get('process_list', '')
        performance_analysis = event_data.get('performance_analysis')
        
        # Создаем запись о событии
        event_entry = f"""
---
### 📈 Пик CPU в {time_str}
- **PID процесса:** `{pid}`
- **Зафиксированная нагрузка:** `{cpu_usage}%`

**Топ-5 запросов по времени выполнения в момент пика:**
{to_markdown_table(process_list)}

"""
        
        # Добавляем анализ производительности, если есть данные
        if performance_analysis:
            event_entry += f"""
**📊 Анализ производительности запросов:**
- **Всего активных запросов:** {performance_analysis['total_queries']}
- **Максимальное время выполнения:** {performance_analysis['max_time']} сек
- **Среднее время выполнения:** {performance_analysis['avg_time']:.1f} сек
- **Медленных запросов (>10 сек):** {len(performance_analysis['slow_queries'])}
- **Критически медленных запросов (>30 сек):** {len(performance_analysis['critical_queries'])}

"""
            
            # Показываем критически медленные запросы
            if performance_analysis['critical_queries']:
                event_entry += "**🚨 Критически медленные запросы (>30 сек):**\n"
                for query in performance_analysis['critical_queries']:
                    event_entry += f"- **{query['TIME']} сек:** {query.get('INFO', 'N/A')[:100]}...\n"
                event_entry += "\n"
            
            # Показываем медленные запросы
            elif performance_analysis['slow_queries']:
                event_entry += "**⚠️ Медленные запросы (>10 сек):**\n"
                for query in performance_analysis['slow_queries']:
                    event_entry += f"- **{query['TIME']} сек:** {query.get('INFO', 'N/A')[:100]}...\n"
                event_entry += "\n"
        
        # Добавляем запись в файл
        with open(report_path, 'a', encoding='utf-8') as f:
            f.write(event_entry)
            
        logger.info(f"Информация о пике CPU добавлена в отчет: {report_path}")
        
    except Exception as e:
        logger.error(f"Ошибка при добавлении информации о пике CPU в отчет: {e}", exc_info=True)

def append_memory_event_to_report(event_data, output_path):
    """Добавляет в отчет событие о высоком потреблении памяти."""
    _ensure_header(output_path)
    
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
        return {}
    with open(events_path, encoding='utf-8') as f:
        text = f.read()
    # Парсим пики CPU
    cpu_usages = []
    all_queries = []
    slow_queries = []
    critical_queries = []
    query_times = []
    query_groups = collections.defaultdict(list)
    # Находим все блоки "Пик CPU ..."
    cpu_blocks = re.split(r'-{3,}', text)
    for block in cpu_blocks:
        cpu_match = re.search(r'Пик CPU.*?Зафиксированная нагрузка:\s*`([\d\.]+)%`', block)
        if cpu_match:
            cpu_usages.append(float(cpu_match.group(1)))
        # Парсим таблицу запросов
        table_match = re.search(r'\|\s*ID\s*\|.*?\n((?:\|.*?\n)+)', block, re.DOTALL)
        if table_match:
            table = table_match.group(1)
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
                except Exception:
                    continue
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
    """
    # Формируем промпт для AI
    prompt = build_ai_prompt(baseline_path, events_path)
    if ENABLE_AI:
        try:
            ai_recommendations = send_to_ai_advisor(prompt)
        except Exception as e:
            ai_recommendations = f"Ошибка при обращении к AI: {e}"
    else:
        ai_recommendations = 'AI отключён настройками.'
    # Агрегируем события
    agg = parse_and_aggregate_events(events_path)
    # Формируем baseline-параметры (только ключевые, без полного baseline)
    key_params = prompt.split('Вот сводка событий за сегодня:')[0].replace('Ты — опытный администратор MySQL. Вот ключевые параметры сервера:', '').strip()
    # Формируем сводку
    summary = []
    cpu_agg = agg.get('cpu_agg', {})
    if cpu_agg.get('count'):
        summary.append(f"**CPU:** среднее: {cpu_agg['avg']:.1f}%, макс: {cpu_agg['max']}%, мин: {cpu_agg['min']}% (пиков: {cpu_agg['count']})")
    query_time_agg = agg.get('query_time_agg', {})
    if query_time_agg.get('count'):
        summary.append(f"**Время выполнения запросов:** среднее: {query_time_agg['avg']:.1f} сек, макс: {query_time_agg['max']} сек, мин: {query_time_agg['min']} сек (всего: {query_time_agg['count']})")
    # Похожие запросы
    if agg.get('grouped_queries'):
        summary.append("**Группы похожих запросов (по INFO):**")
        for g in agg['grouped_queries'][:5]:
            summary.append(f"- {g['INFO']} (всего: {g['count']}, среднее время: {g['avg_time']:.1f} сек, макс: {g['max_time']} сек, мин: {g['min_time']} сек)")
    # Медленные и критические
    if agg.get('critical_queries'):
        summary.append("**Критически медленные запросы (>30 сек):**")
        for q in agg['critical_queries']:
            summary.append(f"- {q['INFO']} (время: {q['TIME']} сек)")
    if agg.get('slow_queries'):
        summary.append("**Медленные запросы (>10 сек):**")
        for q in agg['slow_queries']:
            summary.append(f"- {q['INFO']} (время: {q['TIME']} сек)")
    # Все уникальные запросы (по INFO)
    all_infos = set(q['INFO'] for q in agg.get('grouped_queries', []))
    if all_infos:
        summary.append("\n**Все уникальные запросы за день (по INFO):**")
        for info in all_infos:
            summary.append(f"- {info}")
    summary_str = '\n'.join(summary)
    # Итоговый markdown-отчёт
    report = f"""
# Сводный отчёт за {datetime.now().strftime('%Y-%m-%d')}

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