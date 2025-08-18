#!/bin/sh

set -x  # Включаем режим отладки
echo "Начало выполнения скрипта" > /proc/1/fd/1

# Настраиваем клиент
mc alias set minio http://localhost:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD}

mc mb passport
mc mb terminals
mc mb transaction

mc anonymous set download passport
mc anonymous set download terminals
mc anonymous set download transaction
