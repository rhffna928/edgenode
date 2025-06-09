FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    default-jdk \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /app/logs /app/sqlite_db

RUN chmod +x *.py

ENV PYTHONUNBUFFERED=1 