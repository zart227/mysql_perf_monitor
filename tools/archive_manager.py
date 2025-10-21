#!/usr/bin/env python3
"""
Модуль для автоматической архивации и ротации отчетов и логов.

Функционал:
- Архивация файлов старше ARCHIVE_DAYS_TO_KEEP_UNARCHIVED дней
- Группировка файлов по месяцам в .tar.gz архивы
- Удаление архивов старше ARCHIVE_DAYS_TO_KEEP_ARCHIVED дней
"""

import os
import tarfile
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict
import logging

# Импортируем настройки
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import (
    REPORTS_DIR,
    LOGS_DIR,
    ARCHIVE_ENABLED,
    ARCHIVE_DAYS_TO_KEEP_UNARCHIVED,
    ARCHIVE_DAYS_TO_KEEP_ARCHIVED
)

# Настраиваем логирование
logger = logging.getLogger('mysql_perf_reporter.archive')
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def get_file_age_days(file_path):
    """Возвращает возраст файла в днях."""
    try:
        mtime = os.path.getmtime(file_path)
        file_date = datetime.fromtimestamp(mtime)
        age = datetime.now() - file_date
        return age.days
    except Exception as e:
        logger.error(f"Ошибка при получении возраста файла {file_path}: {e}")
        return 0


def group_files_by_month(files):
    """
    Группирует файлы по месяцам на основе даты в имени файла.
    Возвращает словарь: {(год, месяц): [файлы]}
    """
    grouped = defaultdict(list)
    
    for file_path in files:
        filename = os.path.basename(file_path)
        
        # Извлекаем дату из имени файла (например, events_report_20250627.md -> 20250627)
        # или mysql_perf_reporter.log.20250627 -> 20250627
        date_str = None
        
        # Паттерны для разных типов файлов
        if 'events_report_' in filename or 'daily_summary_' in filename:
            # events_report_20250627.md или daily_summary_20250627.md
            parts = filename.split('_')
            if len(parts) >= 3:
                date_str = parts[-1].replace('.md', '')
        elif 'mysql_perf_reporter.log.' in filename:
            # mysql_perf_reporter.log.20250627
            date_str = filename.split('.')[-1]
        elif filename.startswith('mysql_perf_reporter_'):
            # mysql_perf_reporter_20250627.log
            date_str = filename.replace('mysql_perf_reporter_', '').replace('.log', '')
        elif filename.endswith('.csv'):
            # 2025-06-27.csv (формат для CPU/Memory events)
            date_str = filename.replace('.csv', '').replace('-', '')
        
        if date_str and len(date_str) == 8 and date_str.isdigit():
            try:
                year = int(date_str[:4])
                month = int(date_str[4:6])
                grouped[(year, month)].append(file_path)
            except ValueError:
                logger.warning(f"Не удалось извлечь дату из файла: {filename}")
        else:
            # Если не удалось извлечь дату из имени, используем дату модификации
            try:
                mtime = os.path.getmtime(file_path)
                file_date = datetime.fromtimestamp(mtime)
                grouped[(file_date.year, file_date.month)].append(file_path)
            except Exception as e:
                logger.warning(f"Не удалось определить дату для файла {filename}: {e}")
    
    return grouped


def archive_directory_files(directory, archive_subdir, file_patterns=None, exclude_patterns=None):
    """
    Архивирует старые файлы из директории.
    
    Args:
        directory: Путь к директории для архивации
        archive_subdir: Имя поддиректории для архивов (например, 'archive')
        file_patterns: Список шаблонов файлов для архивации (например, ['*.md', '*.log'])
        exclude_patterns: Список шаблонов для исключения
    """
    if not os.path.exists(directory):
        logger.warning(f"Директория {directory} не существует. Пропускаю.")
        return
    
    archive_dir = os.path.join(directory, archive_subdir)
    os.makedirs(archive_dir, exist_ok=True)
    
    cutoff_date = datetime.now() - timedelta(days=ARCHIVE_DAYS_TO_KEEP_UNARCHIVED)
    
    # Собираем файлы для архивации
    files_to_archive = []
    
    for item in os.listdir(directory):
        item_path = os.path.join(directory, item)
        
        # Пропускаем директории и файлы в директории archive
        if os.path.isdir(item_path):
            continue
        
        # Проверяем возраст файла
        age_days = get_file_age_days(item_path)
        if age_days < ARCHIVE_DAYS_TO_KEEP_UNARCHIVED:
            continue
        
        # Проверяем шаблоны (если указаны)
        if file_patterns:
            # Проверяем, содержит ли имя файла хотя бы один из паттернов
            pattern_match = False
            for pattern in file_patterns:
                clean_pattern = pattern.replace('*', '')
                if clean_pattern in item:
                    pattern_match = True
                    break
            if not pattern_match:
                continue
        
        # Проверяем исключения (точное совпадение имени файла)
        if exclude_patterns:
            if item in exclude_patterns:
                continue
        
        files_to_archive.append(item_path)
    
    if not files_to_archive:
        logger.info(f"В {directory} нет файлов старше {ARCHIVE_DAYS_TO_KEEP_UNARCHIVED} дней для архивации.")
        return
    
    # Группируем по месяцам
    grouped_files = group_files_by_month(files_to_archive)
    
    # Создаем архивы для каждого месяца
    total_archived = 0
    for (year, month), files in grouped_files.items():
        archive_name = f"{os.path.basename(directory)}_{year}_{month:02d}.tar.gz"
        archive_path = os.path.join(archive_dir, archive_name)
        
        try:
            # Если архив уже существует, пропускаем (файлы уже заархивированы)
            if os.path.exists(archive_path):
                logger.info(f"Архив {archive_name} уже существует, пропускаем файлы этого месяца")
                # Удаляем файлы, которые уже в архиве
                for file_path in files:
                    try:
                        os.remove(file_path)
                        total_archived += 1
                        logger.info(f"Удален файл (уже в архиве): {os.path.basename(file_path)}")
                    except Exception as e:
                        logger.error(f"Ошибка при удалении файла {file_path}: {e}")
                continue
            
            # Создаем новый архив
            with tarfile.open(archive_path, 'w:gz') as tar:
                for file_path in files:
                    arcname = os.path.basename(file_path)
                    tar.add(file_path, arcname=arcname)
                    logger.info(f"Добавлен в архив {archive_name}: {arcname}")
            
            # Удаляем заархивированные файлы
            for file_path in files:
                try:
                    os.remove(file_path)
                    total_archived += 1
                    logger.info(f"Удален заархивированный файл: {os.path.basename(file_path)}")
                except Exception as e:
                    logger.error(f"Ошибка при удалении файла {file_path}: {e}")
            
            logger.info(f"Создан архив: {archive_path} ({len(files)} файлов)")
            
        except Exception as e:
            logger.error(f"Ошибка при создании архива {archive_path}: {e}")
    
    logger.info(f"Всего заархивировано и удалено файлов из {directory}: {total_archived}")


def cleanup_old_archives(directory, archive_subdir='archive'):
    """
    Удаляет архивы старше ARCHIVE_DAYS_TO_KEEP_ARCHIVED дней.
    
    Args:
        directory: Путь к директории с архивами
        archive_subdir: Имя поддиректории архивов
    """
    archive_dir = os.path.join(directory, archive_subdir)
    
    if not os.path.exists(archive_dir):
        logger.info(f"Директория архивов {archive_dir} не существует. Пропускаю очистку.")
        return
    
    cutoff_date = datetime.now() - timedelta(days=ARCHIVE_DAYS_TO_KEEP_ARCHIVED)
    deleted_count = 0
    
    for item in os.listdir(archive_dir):
        item_path = os.path.join(archive_dir, item)
        
        if not os.path.isfile(item_path):
            continue
        
        # Проверяем возраст архива
        age_days = get_file_age_days(item_path)
        
        if age_days >= ARCHIVE_DAYS_TO_KEEP_ARCHIVED:
            try:
                os.remove(item_path)
                deleted_count += 1
                logger.info(f"Удален старый архив ({age_days} дней): {item}")
            except Exception as e:
                logger.error(f"Ошибка при удалении архива {item_path}: {e}")
    
    if deleted_count > 0:
        logger.info(f"Удалено старых архивов из {archive_dir}: {deleted_count}")
    else:
        logger.info(f"В {archive_dir} нет архивов старше {ARCHIVE_DAYS_TO_KEEP_ARCHIVED} дней.")


def run_archive_cleanup():
    """
    Основная функция для запуска архивации и очистки.
    Обрабатывает директории reports/ и logs/.
    """
    if not ARCHIVE_ENABLED:
        logger.info("Архивация отключена (ARCHIVE_ENABLED=False). Пропускаю.")
        return
    
    logger.info("="*60)
    logger.info("Запуск процесса архивации и очистки")
    logger.info(f"Настройки: неархивированные файлы хранятся {ARCHIVE_DAYS_TO_KEEP_UNARCHIVED} дней, "
                f"архивы {ARCHIVE_DAYS_TO_KEEP_ARCHIVED} дней")
    logger.info("="*60)
    
    try:
        # Архивация отчетов
        logger.info("\n--- Архивация отчетов (reports/) ---")
        archive_directory_files(
            directory=REPORTS_DIR,
            archive_subdir='archive',
            file_patterns=['.md'],
            exclude_patterns=['baseline_report.md']
        )
        
        # Очистка старых архивов отчетов
        cleanup_old_archives(REPORTS_DIR, 'archive')
        
        # Архивация CPU events
        cpu_events_dir = os.path.join(REPORTS_DIR, 'events', 'cpu')
        if os.path.exists(cpu_events_dir):
            logger.info("\n--- Архивация CPU events (reports/events/cpu/) ---")
            archive_directory_files(
                directory=cpu_events_dir,
                archive_subdir='archive',
                file_patterns=['.csv'],
                exclude_patterns=[]
            )
            cleanup_old_archives(cpu_events_dir, 'archive')
        
        # Архивация Memory events
        memory_events_dir = os.path.join(REPORTS_DIR, 'events', 'memory')
        if os.path.exists(memory_events_dir):
            logger.info("\n--- Архивация Memory events (reports/events/memory/) ---")
            archive_directory_files(
                directory=memory_events_dir,
                archive_subdir='archive',
                file_patterns=['.csv'],
                exclude_patterns=[]
            )
            cleanup_old_archives(memory_events_dir, 'archive')
        
        # Архивация логов
        logger.info("\n--- Архивация логов (logs/) ---")
        archive_directory_files(
            directory=LOGS_DIR,
            archive_subdir='archive',
            file_patterns=['.log'],  # Любые файлы с .log в названии
            exclude_patterns=['mysql_perf_reporter.log']  # Исключаем только текущий активный лог
        )
        
        # Очистка старых архивов логов
        cleanup_old_archives(LOGS_DIR, 'archive')
        
        logger.info("\n" + "="*60)
        logger.info("Процесс архивации и очистки завершен успешно")
        logger.info("="*60 + "\n")
        
    except Exception as e:
        logger.error(f"Критическая ошибка при архивации: {e}", exc_info=True)


if __name__ == '__main__':
    # Позволяет запускать скрипт напрямую
    run_archive_cleanup()

