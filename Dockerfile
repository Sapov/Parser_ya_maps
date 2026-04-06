FROM python:3.10-slim

# Установка UV
RUN pip install --no-cache-dir uv

# Установка Chrome
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    && wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list \
    && apt-get update && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Копируем pyproject.toml
COPY pyproject.toml .

# Установка зависимостей через pyproject.toml
RUN if [ -f "pyproject.toml" ]; then \
        uv pip install --system --no-cache-dir -e .; \
    elif [ -f "requirements.txt" ]; then \
        uv pip install --system --no-cache-dir -r requirements.txt; \
    else \
        echo "No requirements file found!" && exit 1; \
    fi

COPY . .

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]