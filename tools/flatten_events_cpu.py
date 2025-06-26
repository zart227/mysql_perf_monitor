import csv
import re

INPUT_CSV = 'reports/events_cpu.csv'
OUTPUT_CSV = 'reports/events_cpu_fixed.csv'

# Регулярка для парсинга одной строки processlist
PROCESS_ROW_RE = re.compile(r'\|\s*(\d+)\s*\|\s*([^|]+)\|\s*([^|]+)\|\s*([^|]+)\|\s*([^|]+)\|\s*(\d+)\s*\|\s*([^|]+)\|\s*(.*?)\s*\|')

def extract_queries(process_list):
    """
    Парсит текстовую таблицу process_list и возвращает список dict с user, host, time, info.
    """
    queries = []
    if not process_list or not process_list.strip():
        return queries
    for match in PROCESS_ROW_RE.finditer(process_list):
        user = match.group(2).strip()
        host = match.group(3).strip()
        time_val = match.group(6).strip()
        info = match.group(8).strip().replace('\n', ' ').replace('\r', ' ')
        info = re.sub(r'\s+', ' ', info)
        if info and info != 'NULL':
            queries.append({'user': user, 'host': host, 'time_query': time_val, 'info': info})
    return queries

def main():
    with open(INPUT_CSV, newline='', encoding='utf-8') as infile, \
         open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as outfile:
        reader = csv.reader(infile)
        header = next(reader)
        # Определяем, есть ли старый формат (process_list/performance_analysis) или уже плоский
        if 'process_list' in [h.lower() for h in header] or 'performance_analysis' in [h.lower() for h in header]:
            # Старый формат: преобразуем
            fieldnames = ['date', 'time', 'pid', 'cpu', 'user', 'host', 'time_query', 'info']
            writer = csv.DictWriter(outfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
            writer.writeheader()
            for row in csv.DictReader([','.join(header)] + [','.join(r) for r in reader]):
                process_list = row.get('process_list', '')
                if process_list:
                    queries = extract_queries(process_list)
                    for q in queries:
                        writer.writerow({
                            'date': row['date'],
                            'time': row['time'],
                            'pid': row['pid'],
                            'cpu': row['cpu'],
                            'user': q['user'],
                            'host': q['host'],
                            'time_query': q['time_query'],
                            'info': q['info'],
                        })
        else:
            # Уже плоский формат: просто копируем
            writer = csv.writer(outfile, quoting=csv.QUOTE_ALL)
            writer.writerow(header)
            for row in reader:
                writer.writerow(row)

if __name__ == '__main__':
    main() 