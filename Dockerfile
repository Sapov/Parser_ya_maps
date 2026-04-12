FROM python:3.10-slim

# Установка Chrome и системных зависимостей
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    && wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get update && apt-get install -y ./google-chrome-stable_current_amd64.deb \
    && rm google-chrome-stable_current_amd64.deb \
    && rm -rf /var/lib/apt/lists/*


WORKDIR /app

# Копируем requirements.txt
COPY requirements.txt .

# Установка Python зависимостей (используем pip вместо UV для надежности)
RUN pip install --no-cache-dir --upgrade pip
RUN pip install -r requirements.txt


# Копируем весь код
COPY . .

# Проверяем установку
RUN which uvicorn && which celery || (echo "Packages not installed" && exit 1)

# Создаем non-root пользователя
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]