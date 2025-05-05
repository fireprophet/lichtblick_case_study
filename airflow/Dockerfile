# airflow/Dockerfile
FROM apache/airflow:2.7.2-python3.10

# Установка зависимостей
USER airflow
RUN pip install --no-cache-dir \
    boto3 \
    pandas

# Переключаемся на root для копирования и изменения прав
USER root

# Возврат к airflow-пользователю
USER airflow

# Переменные окружения по умолчанию (могут переопределяться через docker-compose)
ENV MINIO_ENDPOINT=http://minio:9000 \
    MINIO_ACCESS_KEY=admin \
    MINIO_SECRET_KEY=admin123 \
    SQLITE_DB_PATH=/opt/airflow/sqlite/etl.db
