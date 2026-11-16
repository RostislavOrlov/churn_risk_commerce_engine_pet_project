#!/usr/bin/env python3

import os
import gc
import json
import psycopg2
import pandas as pd
from data_pipelines.shared.connections import infra
from elasticsearch import helpers

SEEDS_DIR = os.path.dirname(os.path.abspath(__file__))


def ingest_neo4j():
    print("Инициализация и очистка целевой базы данных Neo4j...")
    driver = infra.get_neo4j_driver()
    
    ddl_path = "/opt/pipelines/data_pipelines/database/migrations/neo4j/V1__init_logistics_constraints.cypher"
    if not os.path.exists(ddl_path):
        ddl_path = os.path.abspath(os.path.join(SEEDS_DIR, "..", "migrations", "neo4j", "V1__init_logistics_constraints.cypher"))

    with driver.session(database="neo4j") as session:
        print("Очистка старого логистического графа...")
        session.run("MATCH (n) DETACH DELETE n").consume()
        
        print(f"Применение Constraints и индексов из {os.path.basename(ddl_path)}...")
        with open(ddl_path, "r", encoding="utf-8") as ddl_file:
            clean_lines = [
                line.split("//")[0].strip()
                for line in ddl_file.read().splitlines()
                if line.split("//")[0].strip()
            ]
            statements = [stmt.strip() for stmt in " ".join(clean_lines).split(";") if stmt.strip()]
            for stmt in statements:
                session.run(stmt).consume()

    json_path = os.path.join(SEEDS_DIR, "neo4j", "logistics_graph.json")
    print(f"Запуск прямого пакетного импорта инфраструктуры Neo4j из {os.path.basename(json_path)}...")
    
    with open(json_path, "r", encoding="utf-8") as f:
        graph_data = json.load(f)

    with driver.session(database="neo4j") as session:
        session.run("""
            UNWIND $batch AS row
            MERGE (c:Customer {id: row.id})
        """, batch=graph_data.get("customers", [])).consume()

        for label in ["Warehouse", "Darkstore", "SellerWarehouse"]:
            subset = [o for o in graph_data.get("origins", []) if o["label"] == label]
            if subset:
                session.run(f"""
                    UNWIND $batch AS row
                    MERGE (n:{label}:LogisticsNode {{id: row.id}})
                    SET n.status = 'ACTIVE',
                        n.open_hour = row.open_hour,
                        n.close_hour = row.close_hour,
                        n.is_bottleneck_flag = 0,
                        n.bottleneck_risk_factor = row.risk
                """, batch=subset).consume()

        session.run("""
            UNWIND $batch AS row
            MERGE (n:LogisticsNode {id: row.id})
            SET n.status = 'ACTIVE',
                n.open_hour = row.open_hour,
                n.close_hour = row.close_hour,
                n.is_bottleneck_flag = 0,
                n.bottleneck_risk_factor = row.risk
        """, batch=graph_data.get("transit_hubs", [])).consume()

        session.run("""
            UNWIND $batch AS row
            MERGE (p:PickupPoint:LogisticsNode {id: row.id})
            SET p.status = 'ACTIVE',
                p.local_clustering_coef = row.coef,
                p.open_hour = row.open_hour,
                p.close_hour = row.close_hour,
                p.current_load_pct = row.current_load_pct,
                p.is_bottleneck_flag = 0,
                p.bottleneck_risk_factor = row.risk
        """, batch=graph_data.get("pickup_points", [])).consume()

        session.run("""
            UNWIND $batch AS row
            MATCH (orig:LogisticsNode {id: row.origin_id})
            MATCH (pp:PickupPoint {id: row.pp_id})
            MERGE (orig)-[r:DELIVERY_CORRIDOR]->(pp)
            SET r.transit_nodes_chain = row.chain,
                r.base_link_times = row.times,
                r.base_time = row.base_time,
                r.base_cost = row.base_cost
        """, batch=graph_data.get("corridors", [])).consume()

        session.run("""
            UNWIND $batch AS row
            MATCH (src:LogisticsNode {id: row.src_id})
            MATCH (dst:LogisticsNode {id: row.dst_id})
            MERGE (src)-[leg:LOGISTICS_LEG]->(dst)
            SET leg.base_time = row.base_time,
                leg.congestion = row.congestion,
                leg.status = row.status,
                leg.allowed_categories = row.allowed_categories
        """, batch=graph_data.get("legs", [])).consume()

        session.run("""
            UNWIND $batch AS row
            WITH row, CASE WHEN size(row.chain) > 0 THEN row.chain[-1] ELSE row.origin_id END AS last_hub_id
            MATCH (src:LogisticsNode {id: last_hub_id})
            MATCH (pp:PickupPoint {id: row.pp_id})
            MERGE (src)-[leg:LOGISTICS_LEG]->(pp)
            ON CREATE SET leg.base_time = 2.0,
                          leg.congestion = 1.1,
                          leg.status = 'ACTIVE'
        """, batch=graph_data.get("corridors", [])).consume()

        for w_label in ["WarehouseWorker", "LinehaulDriver", "LastMileCourier"]:
            w_subset = [w for w in graph_data.get("workers", []) if w["label"] == w_label]
            if w_subset:
                session.run(f"""
                    UNWIND $batch AS row
                    MERGE (w:{w_label} {{id: row.id}})
                    SET w += row.props,
                        w.shift_status = row.shift_status,
                        w.calculated_delay = row.calculated_delay
                """, batch=w_subset).consume()

    records = graph_data.get("tracking_events", [])
    chunk_size = 5000
    for i in range(0, len(records), chunk_size):
        batch = records[i:i + chunk_size]
        
        with driver.session(database="neo4j") as session:
            session.run("""
            UNWIND $batch AS row
            MERGE (o:Order {id: row.order_id})
            MERGE (pkg:SKU_Package {id: row.package_id})
                ON CREATE SET pkg.type = row.package_type,
                              pkg.product_id = row.product_id
            MERGE (ev:ScanEvent {id: row.event_id})
            SET ev.status = row.status,
                ev.timestamp = row.timestamp
            MERGE (o)-[:CONTAINS_PACKAGE]->(pkg)
            MERGE (pkg)-[:TRACKED_BY]->(ev)
            
            WITH ev, row
            MERGE (loc:LogisticsNode {id: row.location_id})
            ON CREATE SET loc.status = 'ACTIVE',
                          loc.open_hour = 0.0,
                          loc.close_hour = 24.0,
                          loc.is_bottleneck_flag = 0,
                          loc.bottleneck_risk_factor = 1.0
            MERGE (w:WarehouseWorker {id: row.worker_id})
            MERGE (ev)-[:AT_LOCATION]->(loc)
            MERGE (ev)-[:EXECUTED_BY]->(w)
            """, batch=batch).consume()
            
        print(f"Neo4j: Напрямую импортировано {i + len(batch)} логистических цепочек...")
    
    gc.collect()
    print("Итоговый граф цепей поставок Neo4j успешно прогрет и готов к GDS-аналитике.")

def ingest_postgresql():
    print("Инициализация персистентного DWH слоя Postgres (dw_marts & stg_analytics)...")
    
    db_params = {
        "host": "retail_postgres_master",
        "port": 5432,
        "user": "retail_user",
        "password": "retail_password",
        "database": "retail_oltp"
    }
    
    migrations_dir = "/opt/pipelines/data_pipelines/database/migrations/postgres"
    if not os.path.exists(migrations_dir):
        for folder_name in ["postgres", "postgresql"]:
            candidate_dir = os.path.abspath(os.path.join(SEEDS_DIR, "..", "migrations", folder_name))
            if os.path.exists(candidate_dir):
                migrations_dir = candidate_dir
                break

    conn = psycopg2.connect(**db_params)
    conn.autocommit = True  
    cursor = conn.cursor()
    
    try:
        if os.path.exists(migrations_dir):
            sql_files = sorted([f for f in os.listdir(migrations_dir) if f.endswith(".sql")])
            for sql_file in sql_files:
                file_path = os.path.join(migrations_dir, sql_file)
                print(f"Применение SQL-миграции {sql_file}...")
                with open(file_path, "r", encoding="utf-8") as f:
                    sql_content = f.read()
                    cursor.execute(sql_content)
            print("Все миграции PostgreSQL/TimescaleDB (V1-V3) успешно применены.")
        else:
            print(f"Директория миграций не найдена: {migrations_dir}")

        conn.autocommit = False
        cursor.execute("SET session_replication_role = 'replica';")
        
        cursor.execute("TRUNCATE TABLE dw_marts.dwh_user_ltv CASCADE;")
        ltv_csv = os.path.join(SEEDS_DIR, "postgresql", "dwh_user_ltv.csv")
        with open(ltv_csv, 'r', encoding='utf-8') as f:
            cursor.copy_expert(
                "COPY dw_marts.dwh_user_ltv (user_id, long_term_value_rub, customer_segment, updated_at) FROM STDIN WITH CSV HEADER",
                f
            )
        print("Витрина dw_marts.dwh_user_ltv успешно заполнена.")
        
        cursor.execute("TRUNCATE TABLE dw_marts.dwh_market_margins CASCADE;")
        margin_csv = os.path.join(SEEDS_DIR, "postgresql", "dwh_market_margins.csv")
        with open(margin_csv, 'r', encoding='utf-8') as f:
            cursor.copy_expert(
                "COPY dw_marts.dwh_market_margins (order_id, order_margin_rub, is_express_grocery, updated_at) FROM STDIN WITH CSV HEADER",
                f
            )
        print("Витрина dw_marts.dwh_market_margins успешно заполнена.")

        cursor.execute("TRUNCATE TABLE public.orders CASCADE;")
        orders_csv = os.path.join(SEEDS_DIR, "postgresql", "orders.csv")
        with open(orders_csv, 'r', encoding='utf-8') as f:
            cursor.copy_expert(
                "COPY public.orders (order_id, user_id, status, created_at) FROM STDIN WITH CSV HEADER",
                f
            )
        print("Таблица связок public.orders успешно заполнена.")

        
        conn.commit()
    except Exception as e:
        if not conn.autocommit:
            conn.rollback()
        print(f"Критический сбой загрузки в PostgreSQL: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()


import urllib.request
import urllib.parse

def ingest_clickhouse():
    print("Инициализация OLAP слоя ClickHouse (Нативный HTTP-накат DDL + Прогрев)...")
    
    def run_clickhouse_query(sql_query: str):
        print(sql_query)
        url = "http://clickhouse:8123/?" + urllib.parse.urlencode({
            "user": "clickhouse_user",
            "password": "clickhouse_password",
            "database": "retail_olap"
        })
        req = urllib.request.Request(url, data=sql_query.encode('utf-8'), method='POST')
        try:
            with urllib.request.urlopen(req) as response:
                return response.read().decode('utf-8')
        except urllib.error.HTTPError as e:
            err_body = e.read().decode('utf-8', errors='replace')
            print(f"ClickHouse HTTP 500 на запросе:\n{sql_query[:200]}\nДетали от сервера: {err_body}")
            raise e


    print("Создание баз данных пространств имен через HTTP...")
    run_clickhouse_query("CREATE DATABASE IF NOT EXISTS s2_telemetry;")
    run_clickhouse_query("CREATE DATABASE IF NOT EXISTS s2_analytics;")
    run_clickhouse_query("CREATE DATABASE IF NOT EXISTS s2_shipping;")
    
    ddl_path = "/opt/pipelines/data_pipelines/database/migrations/clickhouse/V1__init_source_tables.sql"
    
    if not os.path.exists(ddl_path):
        ddl_path = os.path.abspath(os.path.join(SEEDS_DIR, "..", "migrations", "clickhouse", "V1__init_source_tables.sql"))
        
    print(f"Применение основного файла DDL-миграции...")
    with open(ddl_path, "r", encoding="utf-8") as ddl_file:
        content = ddl_file.read()
        sql_commands = content.split(";")
        for command in sql_commands:
            clean_command = command.strip()
            if clean_command and not clean_command.startswith("--"):
                run_clickhouse_query(clean_command)

    print("Очистка старых данных дисковых таблиц MergeTree...")
    run_clickhouse_query("TRUNCATE TABLE IF EXISTS default.user_clicks;")
    run_clickhouse_query("TRUNCATE TABLE IF EXISTS s2_telemetry.web_performance_logs;")
    run_clickhouse_query("TRUNCATE TABLE IF EXISTS s2_analytics.search_funnel_logs;")
    run_clickhouse_query("TRUNCATE TABLE IF EXISTS s2_shipping.clickstream_scan_events;")
    
    print("Пакетная векторизованная заливка Big Data семян...")
    client = infra.get_clickhouse_client()
    load_clickhouse_json_file(client, "default.user_clicks", os.path.join(SEEDS_DIR, "clickhouse", "user_clicks.json"))
    load_clickhouse_json_file(client, "s2_telemetry.web_performance_logs", os.path.join(SEEDS_DIR, "clickhouse", "web_performance_logs.json"))
    load_clickhouse_json_file(client, "s2_analytics.search_funnel_logs", os.path.join(SEEDS_DIR, "clickhouse", "search_funnel_logs.json"))
    load_clickhouse_json_file(client, "s2_shipping.clickstream_scan_events", os.path.join(SEEDS_DIR, "clickhouse", "clickstream_scan_events.json"))
    
    print("[CLICKHOUSE SUCCESS] Схемы созданы, таблицы развернуты, данные на диске!")


from datetime import datetime

def load_clickhouse_json_file(client, table_name: str, filepath: str):
    print(f"ClickHouse: Загрузка семян в таблицу {table_name}...")
    with open(filepath, "r", encoding="utf-8") as f:
        batch = []
        for line in f:
            if line.strip():
                row_dict = json.loads(line)
                row_values = list(row_dict.values())
                
                for idx, val in enumerate(row_values):
                    if isinstance(val, str):
                        if len(val) == 10 and val[4] == "-" and val[7] == "-":
                            try:
                                row_values[idx] = datetime.strptime(val, "%Y-%m-%d").date()
                            except ValueError:
                                pass
                        elif len(val) == 19 and val[4] == "-" and val[10] == " ":
                            try:
                                row_values[idx] = datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
                            except ValueError:
                                pass
                        
                batch.append(row_values)
                
                if len(batch) >= 20000:
                    client.insert(table_name, batch)
                    batch = []
        if batch:
            client.insert(table_name, batch)


def ingest_elasticsearch():
    print("Инициализация Distributed Search слоя Elasticsearch (Strict Mapping)...")
    es = infra.get_elasticsearch_client()
    index_name = "retail_catalog_v2"

    print("Ожидание готовности Cluster State Elasticsearch...")
    es.cluster.health(wait_for_status="yellow", request_timeout=60)
    
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
        
    strict_mapping = {
        "dynamic": "strict",
        "properties": {
            "product_id": {"type": "keyword"},
            "base_price": {"type": "double"},
            "marketing_tags": {"type": "keyword"},
            "bundle_compatible_ids": {"type": "keyword"},
            "category_hierarchy": {
                "properties": {
                    "category_id": {"type": "keyword"},
                    "category_name": {"type": "text"},
                    "level": {"type": "integer"}
                }
            },
            "active_promotions": {
                "type": "nested",
                "properties": {
                    "promo_campaign_id": {"type": "keyword"},
                    "start_date": {"type": "date", "format": "yyyy-MM-dd"},
                    "end_date": {"type": "date", "format": "yyyy-MM-dd"},
                    "status": {"type": "keyword"},
                    "discount_type": {"type": "keyword"},
                    "discount_value": {"type": "double"}
                }
            }
        }
    }
    
    es.indices.create(index=index_name, mappings=strict_mapping)
    
    json_path = os.path.join(SEEDS_DIR, "elasticsearch", "retail_catalog_v2.json")
    with open(json_path, "r", encoding="utf-8") as f:
        catalog_data = json.load(f)
        
    def generate_actions():
        for doc in catalog_data:
            yield {
                "_index": index_name,
                "_source": doc
            }
            
    helpers.bulk(es, generate_actions())
    print(f"Индекс '{index_name}' успешно инициализирован и наполнен.")


def main():
    print("\n[START] Инициализация сквозного синхронного Big Data сидера баз данных...")

    ingest_clickhouse()
    gc.collect()

    ingest_elasticsearch()
    gc.collect()

    ingest_neo4j()
    infra.close_neo4j_driver()
    gc.collect()

    ingest_postgresql()

    print("\n[SUCCESS] Абсолютно все СУБД успешно просидированы напрямую в синхронном режиме!")

if __name__ == "__main__":
    main()
