# Стадия 1: Установка зависимостей
FROM python:3.10-slim as builder


# Установка UV
RUN pip install uv

WORKDIR /app

# Копируем файлы зависимостей
COPY pyproject.toml uv.lock*  ./

# Установка зависимостей в виртуальное окружение
RUN uv venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN uv pip install --no-cache -r requirements.txt

# Стадия 2: Финальный образ
FROM python:3.10-slim

# Установка Chrome и зависимостей
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    && wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list \
    && apt-get update && apt-get install -y google-chrome-stable \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Копируем виртуальное окружение из билдера
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /app

# Копируем код приложения
COPY . .

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]