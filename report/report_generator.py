from jinja2 import Template
from datetime import datetime
import os
import pandas as pd
import io
import re
import logging

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