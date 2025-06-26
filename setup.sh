#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Ensure venv is working ---
echo "Проверка работоспособности модуля venv..."
if python3 -m venv .test_venv &> /dev/null; then
    rm -rf .test_venv
    echo "Модуль venv работает корректно."
else
    echo "Модуль venv не работает. Попытка установки..."
    if command -v apt-get &> /dev/null; then
        PY_VERSION_PKG=$(python3 -c 'import sys; print(f"python{sys.version_info.major}.{sys.version_info.minor}-venv")')
        echo "Попытка установки пакета ${PY_VERSION_PKG} через apt-get..."
        sudo apt-get update && sudo apt-get install -y "$PY_VERSION_PKG"
    else
        echo "Система управления пакетами apt-get не найдена. Пожалуйста, установите пакет 'venv' для вашего Python 3 вручную."
        exit 1
    fi

    echo "Повторная проверка после установки..."
    if ! python3 -m venv .test_venv &> /dev/null; then
        echo "Не удалось настроить venv. Пожалуйста, проверьте конфигурацию Python."
        exit 1
    fi
    rm -rf .test_venv
    echo "Модуль venv успешно настроен."
fi

# --- Create virtual environment ---
if [ ! -d "venv" ]; then
  echo "Создание виртуального окружения..."
  python3 -m venv venv
else
  echo "Виртуальное окружение 'venv' уже существует."
fi


# --- Activate virtual environment and install dependencies ---
echo "Активация виртуального окружения и установка зависимостей..."
source venv/bin/activate
pip install -r requirements.txt

echo "Настройка завершена. Виртуальное окружение 'venv' готово, зависимости установлены."
echo "Для активации виртуального окружения в текущей сессии выполните: source venv/bin/activate" 