#!/bin/bash

echo "⚠ Полная очистка Docker! Это удалит все контейнеры, образы, тома и сети!"
read -p "Вы уверены? (y/N): " confirmation

if [[ "$confirmation" != "y" && "$confirmation" != "Y" ]]; then
    echo "Операция отменена."
    exit 1
fi

echo "🛑 Останавливаем все запущенные контейнеры..."
docker stop $(docker ps -aq)

echo "🗑 Удаляем все контейнеры..."
docker rm $(docker ps -aq)

echo "🗑 Удаляем все образы..."
docker rmi -f $(docker images -q)

echo "🧹 Удаляем все тома..."
docker volume rm $(docker volume ls -q)

echo "🌐 Удаляем все неиспользуемые сети..."
docker network rm $(docker network ls -q)

echo "🚀 Очистка кеша Docker..."
docker system prune -af --volumes

echo "✅ Полная очистка Docker завершена!"
