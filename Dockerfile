FROM python:3.10-slim

WORKDIR /app

RUN pip install uv

# Копируем оба файла
COPY pyproject.toml uv.lock ./

# Установка с локком
RUN uv pip install --system --no-cache -r pyproject.toml

# Копируем код приложения
COPY . /app/

# Создаем пользователя и даем права на /app
RUN addgroup --system --gid 1000 appuser && \
    adduser --system --uid 1000 --ingroup appuser appuser && \
    chown -R appuser:appuser /app

# Переключаемся на пользователя
USER 1000

CMD ["python", "server.py"]
