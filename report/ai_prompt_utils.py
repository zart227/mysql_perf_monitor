import re
import os
import pandas as pd
import io

def extract_key_params_from_baseline(baseline_path):
    """Извлекает ключевые параметры из baseline_report.md (глобальные переменные и CPU info)."""
    if not os.path.exists(baseline_path):
        return {}
    with open(baseline_path, encoding='utf-8') as f:
        text = f.read()
    # Глобальные переменные
    global_vars = {}
    match = re.search(r'## Глобальные переменные MySQL\n(.*?)\n#|\Z', text, re.DOTALL)
    if match:
        table = match.group(1)
        if table:
            # Очищаем markdown-таблицу: убираем строки-разделители и пустые строки
            lines = [l for l in table.strip().splitlines() if l.strip() and not l.strip().startswith('|:')]
            if lines and lines[0].startswith('|'):
                # Если первая строка начинается с |, удаляем лишние пробелы
                lines = [l.strip() for l in lines]
            clean_table = '\n'.join(lines)
            try:
                df = pd.read_csv(io.StringIO(clean_table), sep='|', engine='python')
                df = df.dropna(axis=1, how='all')
                df.columns = [c.strip() for c in df.columns]
                for param in [
                    'version', 'innodb_buffer_pool_size', 'key_buffer_size', 'query_cache_size', 'max_connections',
                    'table_open_cache', 'tmp_table_size', 'max_heap_table_size', 'storage_engine', 'character_set_server',
                    'collation_server', 'wait_timeout', 'log_slow_queries', 'slow_query_log_file', 'general_log', 'innodb_file_per_table']:
                    row = df[df['Variable_name'].str.strip() == param]
                    if not row.empty:
                        global_vars[param] = row.iloc[0]['Value']
            except Exception as e:
                global_vars['parse_error'] = f'Ошибка парсинга таблицы: {e}'
    # CPU info
    cpu_info = {}
    cpu_match = re.search(r'## Информация о CPU\n(.*?)\n---', text, re.DOTALL)
    if cpu_match:
        cpu_table = cpu_match.group(1)
        cpu_lines = [l for l in cpu_table.split('\n') if '|' in l and ':' not in l]
        for line in cpu_lines:
            parts = [p.strip() for p in line.split('|') if p.strip()]
            if len(parts) == 2:
                cpu_info[parts[0]] = parts[1]
    return {'global_vars': global_vars, 'cpu_info': cpu_info}

def extract_summary_from_events(events_path):
    """Извлекает сводку проблем, статистику и топ-запросы из events_report_YYYYMMDD.md."""
    if not os.path.exists(events_path):
        return ''
    with open(events_path, encoding='utf-8') as f:
        text = f.read()
    # Найденные проблемы
    problems = re.findall(r'### Проблемы:(.*?)###', text, re.DOTALL)
    problems = problems[0].strip() if problems else ''
    # Рекомендации
    recs = re.findall(r'### Рекомендации:(.*?)---', text, re.DOTALL)
    recs = recs[0].strip() if recs else ''
    # Топ-5 запросов
    top_queries = re.findall(r'Топ-5 запросов.*?\n(.*?)\n---', text, re.DOTALL)
    top_queries = top_queries[0].strip() if top_queries else ''
    # CPU/память
    cpu_events = re.findall(r'Пик CPU.*?\n(.*?)\n---', text, re.DOTALL)
    mem_events = re.findall(r'Высокое потребление памяти.*?\n(.*?)\n---', text, re.DOTALL)
    summary = f"Проблемы:\n{problems}\n\nРекомендации:\n{recs}\n\nТоп-5 запросов:\n{top_queries}\n\nCPU события:\n{'; '.join(cpu_events)}\nПамять события:\n{'; '.join(mem_events)}"
    return summary

def build_ai_prompt(baseline_path, events_path):
    params = extract_key_params_from_baseline(baseline_path)
    summary = extract_summary_from_events(events_path)
    global_vars = params.get('global_vars', {})
    cpu_info = params.get('cpu_info', {})
    # Формируем текст ключевых параметров
    key_params = []
    for k, v in global_vars.items():
        key_params.append(f"{k}: {v}")
    for k, v in cpu_info.items():
        if k.lower() in ['model name', 'cpu cores', 'cpu mhz']:
            key_params.append(f"{k}: {v}")
    key_params_str = '\n'.join(key_params)
    prompt = f"""
Ты — опытный администратор MySQL. Вот ключевые параметры сервера:
{key_params_str}

Вот сводка событий за сегодня:
{summary}

Проблема: В течение дня наблюдались пики CPU, долгие запросы, высокая загрузка памяти и другие инциденты.

Что нужно:
Дай подробные рекомендации для администратора MySQL:
- Что критично и требует немедленного внимания?
- Какие параметры или запросы стоит оптимизировать?
- Какие действия предпринять в первую очередь?
- Если есть типовые ошибки конфигурации — укажи их.
"""
    return prompt 