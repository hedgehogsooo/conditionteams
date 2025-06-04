FROM python:3.12-slim

WORKDIR /app

# Установка зависимостей
RUN apt-get update && \
    apt-get install -y curl && \
    rm -rf /var/lib/apt/lists/*

# Копирование тестов
COPY test_mission.py .
COPY requirements.txt .

# Установка Python-зависимостей
RUN pip install --no-cache-dir -r requirements.txt

# Скрипт ожидания сервисов
COPY wait-for-services.sh .
RUN chmod +x wait-for-services.sh

CMD ["./wait-for-services.sh", "python", "test_mission.py"]