#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Update pip ---
echo "Обновление pip..."
python3 -m pip install --upgrade pip

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