#!/usr/bin/env python3
"""
Скрипт для ручного запуска архивации и очистки отчетов/логов.

Использование:
    python tools/run_archive.py
    
или:
    python -m tools.run_archive
"""

import os
import sys

# Добавляем корневую директорию проекта в путь
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.archive_manager import run_archive_cleanup

if __name__ == '__main__':
    print("Запуск процедуры архивации и очистки...")
    print("=" * 60)
    
    try:
        run_archive_cleanup()
        print("\n" + "=" * 60)
        print("Архивация завершена успешно!")
        sys.exit(0)
    except Exception as e:
        print(f"\n[ОШИБКА] Не удалось выполнить архивацию: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

