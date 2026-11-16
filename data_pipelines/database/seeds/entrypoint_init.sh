#!/bin/bash
set -e

echo "⏳ [INIT] Ожидание полной сетевой готовности сокетов всего стека СУБД..."

wait_for_port() {
    local host="${1}"
    local port="${2}"
    echo "  🔎 Проверка линка к ${host}:${port}..."
    until bash -c "cat < /dev/null > /dev/tcp/${host}/${port}" 2>/dev/null; do
        echo "    ⏳ Сервис ${host}:${port} еще недоступен. Ожидание 3 секунды..."
        sleep 3
    done
    echo "Сервис ${host}:${port} успешно ответил на сокет-запрос!"
}

wait_for_port "retail_postgres_master" 5432
wait_for_port "clickhouse" 8123
wait_for_port "elasticsearch" 9200
wait_for_port "ecom_neo4j" 7687

echo "[INIT] Шаг 1: Запуск векторизованной генерации сквозных Big Data семян..."
python3 /opt/pipelines/data_pipelines/database/seeds/generate_synthetic_data.py

echo "[INIT] Шаг 2: Накат DDL-схем и пакетная загрузка во ВСЕ СУБД (Postgres, CH, ES, Neo4j)..."
python3 /opt/pipelines/data_pipelines/database/seeds/load_synthetic_data.py

echo "[SUCCESS] Вся High-Load инфраструктура успешно инициализирована, связана и засидирована!"
