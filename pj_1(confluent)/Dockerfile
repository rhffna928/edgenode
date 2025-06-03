FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    default-jdk \
    && rm -rf /var/lib/apt/lists/* \ 
    mkdir -p /app/logs /app/sqlite_db \
    chmod +x *.py \
    adduser -D user

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1 

USER user