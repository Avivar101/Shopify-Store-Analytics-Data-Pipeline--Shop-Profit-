FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV UV_PROJECT_ENVIRONMENT=/opt/venv

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install --no-cache-dir uv

# Copy dependency files first for better build caching
COPY pyproject.toml uv.lock* ./

# Install dependencies
RUN uv sync --frozen || uv sync

# Copy project
COPY . .

# Ensure Dagster home exists
RUN mkdir -p /app/.dagster /app/db

EXPOSE 3000

CMD [ "uv", "run", "dagster", "dev", "-h", "0.0.0.0", "-p", "3000", "-m", "orchestration.definitions" ]