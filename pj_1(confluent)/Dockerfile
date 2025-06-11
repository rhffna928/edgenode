FROM python:3.9-slim

ARG HOST_UID=1000
ARG HOST_GID=1000

WORKDIR /app

RUN apt-get update && apt-get install -y \
    default-jdk \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /app/logs /app/sqlite_db

RUN chmod +x *.py

ENV PYTHONUNBUFFERED=1 
RUN addgroup --gid $HOST_GID user
RUN adduser --system --uid $HOST_UID --ingroup user --no-create-home user

USER user