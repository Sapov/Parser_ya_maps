# # 1. Этап сборки (builder)
# FROM python:3.12-slim-bookworm AS builder
#
# # Установка uv из официального образа
# COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv
#
# # Настройка окружения для сборки
# ENV UV_COMPILE_BYTECODE=1
# ENV UV_LINK_MODE=copy
#
# WORKDIR /app
#
# # Копируем файлы зависимостей
# COPY uv.lock pyproject.toml /app/
#
# # Устанавливаем зависимости (без исходного кода проекта)
# RUN --mount=type=cache,target=/root/.cache/uv \
#     uv sync --frozen --no-install-project
#
# # Копируем исходный код
# COPY . /app
#
# # Синхронизируем проект (устанавливаем сам проект)
# RUN --mount=type=cache,target=/root/.cache/uv \
#     uv sync --frozen
#
# # 2. Этап запуска (runner)
# FROM python:3.12-slim-bookworm
#
#
# # Копируем только виртуальное окружение из builder
# COPY --from=builder /app/.venv /app/.venv
#
#
# # Добавляем виртуальное окружение в PATH
# ENV PATH="/app/.venv/bin:$PATH"
#
# # Установка Chrome
# RUN apt-get update && apt-get install -y \
#     wget \
#     gnupg \
#     unzip \
#     && wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
#     && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list \
#     && apt-get update && apt-get install -y google-chrome-stable \
#     && rm -rf /var/lib/apt/lists/*
#
#
# COPY . .
#
# CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]


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
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir fastapi uvicorn celery redis && \
    pip install --no-cache-dir -r requirements.txt || \
    pip install --no-cache-dir selenium undetected-chromedriver sqlalchemy aiohttp aiosqlite beautifulsoup4 lxml pydantic python-dotenv

# Копируем весь код
COPY . .

# Проверяем установку
RUN which uvicorn && which celery || (echo "Packages not installed" && exit 1)

# Создаем non-root пользователя
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]