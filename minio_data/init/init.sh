#!/bin/sh

echo "⏳ Ожидание запуска MinIO..."
sleep 5

/init/mc alias set local http://minio:9000 admin admin123

/init/mc mb -p local/srcdata || echo "Бакет уже существует"

/init/mc cp --recursive /srcdata/ local/srcdata

echo "✅ Файлы загружены в бакет 'srcdata'"
