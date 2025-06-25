import os
import time
import sys
from datetime import datetime, timedelta

LOG_PATH = '/app/logs/mysql_perf_reporter.log'
HEARTBEAT_STR = 'HEARTBEAT'
CHECK_INTERVAL = 60  # секунд
HEARTBEAT_TIMEOUT = 120  # секунд


def find_last_heartbeat():
    if not os.path.exists(LOG_PATH):
        return None
    try:
        with open(LOG_PATH, 'r', encoding='utf-8') as f:
            lines = f.readlines()[-200:]
        for line in reversed(lines):
            if HEARTBEAT_STR in line:
                # Пример: 2025-06-24 05:00:41,868 [INFO] HEARTBEAT: ...
                try:
                    ts_str = line.split(' [')[0]
                    ts = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S,%f')
                    return ts
                except Exception:
                    continue
        return None
    except Exception as e:
        print(f"[WATCHDOG] Ошибка при чтении лога: {e}")
        return None

def kill_main_process():
    # Используем /proc для поиска процессов вместо ps
    import subprocess
    try:
        # Сначала пробуем найти процесс через /proc
        for pid_dir in os.listdir('/proc'):
            if pid_dir.isdigit():
                try:
                    cmdline_path = f'/proc/{pid_dir}/cmdline'
                    if os.path.exists(cmdline_path):
                        with open(cmdline_path, 'r') as f:
                            cmdline = f.read()
                            if 'python' in cmdline and 'main.py' in cmdline and 'watchdog' not in cmdline:
                                pid = int(pid_dir)
                                print(f"[WATCHDOG] Завершаю процесс main.py, PID={pid}")
                                os.kill(pid, 9)
                                return True
                except (IOError, OSError, ValueError):
                    continue
        
        # Если не нашли через /proc, пробуем через ps (если доступен)
        try:
            out = subprocess.check_output(["ps", "aux"]).decode()
            for line in out.splitlines():
                if 'python' in line and 'main.py' in line and 'watchdog' not in line:
                    pid = int(line.split()[1])
                    print(f"[WATCHDOG] Завершаю процесс main.py, PID={pid}")
                    os.kill(pid, 9)
                    return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            pass
            
        print("[WATCHDOG] Не найден процесс main.py для завершения.")
        return False
    except Exception as e:
        print(f"[WATCHDOG] Ошибка при завершении процесса: {e}")
        return False

def main():
    print("[WATCHDOG] Старт watchdog...")
    while True:
        last_heartbeat = find_last_heartbeat()
        now = datetime.now()
        if last_heartbeat is None or (now - last_heartbeat).total_seconds() > HEARTBEAT_TIMEOUT:
            print(f"[WATCHDOG] Не найден свежий heartbeat! Последний: {last_heartbeat}. Перезапуск...")
            kill_main_process()
            time.sleep(10)
        else:
            print(f"[WATCHDOG] Всё ок. Последний heartbeat: {last_heartbeat}")
        time.sleep(CHECK_INTERVAL)

if __name__ == '__main__':
    main() 