# Используем официальный минимальный образ Python
FROM python:3.10-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# Устанавливаем tzdata и настраиваем московское время
RUN apt-get update && \
    apt-get install -y tzdata coreutils wget unzip && \
    ln -fs /usr/share/zoneinfo/Europe/Moscow /etc/localtime && \
    dpkg-reconfigure -f noninteractive tzdata && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV TZ=Europe/Moscow

# Копируем зависимости
COPY requirements.txt ./

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Копируем исходный код
COPY . .

# Переменные окружения для python
ENV PYTHONUNBUFFERED=1

# Установка Xray-core
RUN apt-get update && apt-get install -y wget unzip && \
    wget -O /tmp/xray.zip https://github.com/XTLS/Xray-core/releases/download/v25.6.8/Xray-linux-64.zip && \
    unzip /tmp/xray.zip -d /opt/xray && \
    chmod +x /opt/xray/xray && \
    rm /tmp/xray.zip

# Копируем конфиг VLESS
COPY xray_vless_config.json /opt/xray/xray_vless_config.json

# Установка requests с поддержкой socks
RUN pip install --no-cache-dir 'requests[socks]'

# Добавляю healthcheck-скрипт
COPY healthcheck_xray.py /opt/xray/healthcheck_xray.py

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 CMD python3 /opt/xray/healthcheck_xray.py

# Запуск Xray и приложения
CMD /opt/xray/xray -c /opt/xray/xray_vless_config.json & python main.py 