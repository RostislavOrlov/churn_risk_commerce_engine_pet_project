#!/usr/bin/env python3

import os
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

print("Инициализация Enterprise-генератора. Зачистка инфраструктурного стейта...")

SEEDS_DIR = os.path.dirname(os.path.abspath(__file__))

TARGET_SEEDS = [
    os.path.join(SEEDS_DIR, "postgresql", "dwh_user_ltv.csv"),
    os.path.join(SEEDS_DIR, "postgresql", "dwh_market_margins.csv"),
    os.path.join(SEEDS_DIR, "clickhouse", "user_clicks.json"),
    os.path.join(SEEDS_DIR, "clickhouse", "web_performance_logs.json"),
    os.path.join(SEEDS_DIR, "clickhouse", "search_funnel_logs.json"),
    os.path.join(SEEDS_DIR, "clickhouse", "clickstream_scan_events.json"),
    os.path.join(SEEDS_DIR, "elasticsearch", "retail_catalog.json"),
    os.path.join(SEEDS_DIR, "neo4j", "logistics_graph.json")
]

for folder in ["postgresql", "clickhouse", "elasticsearch", "neo4j"]:
    os.makedirs(os.path.join(SEEDS_DIR, folder), exist_ok=True)

for file_path in TARGET_SEEDS:
    if os.path.exists(file_path):
        try:
            os.remove(file_path)
            print(f"Удален устаревший дамп: {os.path.basename(file_path)}")
        except Exception as e:
            print(f"Ошибка очистки файла {os.path.basename(file_path)}: {e}")

print("⏳ Запуск векторизованного пайплайна генерации...")

np.random.seed(42)

num_users = 2500
num_products = 400
num_orders = 15000

user_ids = [f"USR_{i:07d}" for i in range(1, num_users + 1)]
product_ids = [f"PRD_{i:07d}" for i in range(1, num_products + 1)]
order_ids = [f"ORD_{i:07d}" for i in range(1, num_orders + 1)]
event_ids = [f"EVT_{i:08d}" for i in range(1, num_orders + 1)]
package_ids = [f"PKG_{i:08d}" for i in range(1, num_orders + 1)]

order_user_map = np.random.choice(user_ids, size=num_orders)
order_product_map = np.random.choice(product_ids, size=num_orders)

darkstore_nodes = [f"DS_{i:03d}" for i in range(1, 15)]
executor_nodes = [f"EXE_{i:04d}" for i in range(1, 50)]
executor_types = ["WAREHOUSE_WORKER", "LINEHAUL_DRIVER", "LAST_MILE_COURIER"]

base_time = datetime(2026, 11, 9, 0, 0, 0)
target_date_str = base_time.strftime("%Y-%m-%d")

print("[1/4] ГЕНЕРАЦИЯ: PostgreSQL DWH Layer...")

df_pg_ltv = pd.DataFrame({
    'user_id': user_ids,
    'long_term_value_rub': np.random.uniform(3000.0, 450000.0, size=num_users).round(2),
    'customer_segment': np.random.choice(['VIP_RETENTION', 'REGULAR', 'CHURN_RISK', 'NEW_USER'], size=num_users, p=[0.10, 0.60, 0.15, 0.15]),
    'updated_at': [base_time.isoformat()] * num_users
})
df_pg_ltv.to_csv(os.path.join(SEEDS_DIR, "postgresql", "dwh_user_ltv.csv"), index=False)

df_pg_margins = pd.DataFrame({
    'order_id': order_ids,
    'order_margin_rub': np.random.uniform(-150.0, 4500.0, size=num_orders).round(2),
    'is_express_grocery': np.random.choice([0, 1], size=num_orders, p=[0.75, 0.25]),
    'updated_at': [base_time.isoformat()] * num_orders
})
df_pg_margins.to_csv(os.path.join(SEEDS_DIR, "postgresql", "dwh_market_margins.csv"), index=False)

ordering_users = np.random.choice(user_ids, size=max(1, int(num_users * 0.75)), replace=False)
order_timestamps = [
    (base_time - pd.Timedelta(minutes=int(m))).isoformat()
    for m in np.random.randint(1, 1440, size=num_orders)
]
df_pg_orders = pd.DataFrame({
    'order_id': order_ids,
    'user_id': np.random.choice(ordering_users, size=num_orders, replace=True),
    'status': np.random.choice(['CREATED', 'IN_TRANSIT', 'DELIVERED'], size=num_orders, p=[0.20, 0.65, 0.15]),
    'created_at': order_timestamps
})
df_pg_orders.to_csv(os.path.join(SEEDS_DIR, "postgresql", "orders.csv"), index=False)

print("[2/4] ГЕНЕРАЦИЯ: ClickHouse Stream Sources (JSONEachRow)...")

def save_json_each_row(df: pd.DataFrame, filepath: str):
    with open(filepath, 'w', encoding='utf-8') as f:
        for record in df.to_dict(orient='records'):
            f.write(json.dumps(record, ensure_ascii=False) + '\n')

df_ch_clicks = pd.DataFrame({
    'log_date': [target_date_str] * 50000, 
    'user_id': np.random.choice(user_ids, size=50000),
    'timestamp': [(base_time + timedelta(seconds=int(i * 1.5))).strftime("%Y-%m-%d %H:%M:%S") for i in range(50000)],
    'product_id': np.random.choice(product_ids, size=50000),
    'warehouse_id': [f"WH_{np.random.randint(1,5):02d}" for _ in range(50000)],
    'pickup_point_id': [f"PP_{np.random.randint(1,20):03d}" for _ in range(50000)],
    'page_type': np.random.choice(['catalog', 'product_card', 'cart', 'checkout'], size=50000),
    'category_id': np.random.randint(1, 20, size=50000, dtype=np.uint32)
})
save_json_each_row(df_ch_clicks, os.path.join(SEEDS_DIR, "clickhouse", "user_clicks.json"))

df_ch_perf = pd.DataFrame({
    'log_date': [target_date_str] * 30000,  
    'user_id': np.random.choice(user_ids, size=30000),
    'timestamp': [(base_time + timedelta(seconds=int(i * 2.5))).strftime("%Y-%m-%d %H:%M:%S") for i in range(30000)],
    'status_code': np.random.choice([200, 404, 500, 503], size=30000, p=[0.92, 0.05, 0.02, 0.01]).astype(np.uint16),
    'page_load_ms': np.random.exponential(scale=180, size=30000).astype(np.uint32) + 25,
    'dns_resolution_ms': np.random.randint(2, 45, size=30000, dtype=np.uint32)
})
df_ch_perf.loc[df_ch_perf['status_code'] >= 500, 'page_load_ms'] += 5000
save_json_each_row(df_ch_perf, os.path.join(SEEDS_DIR, "clickhouse", "web_performance_logs.json"))

df_ch_funnel = pd.DataFrame({
    'log_date': [target_date_str] * 40000, 
    'user_id': np.random.choice(user_ids, size=40000),
    'timestamp': [(base_time + timedelta(seconds=int(i * 2.0))).strftime("%Y-%m-%d %H:%M:%S") for i in range(40000)],
    'action_type': np.random.choice(['SEARCH_QUERY', 'FILTER_APPLY', 'SORT_PRICE', 'PROMO_CLICK'], size=40000),
    'scroll_depth_items': np.random.randint(1, 150, size=40000, dtype=np.uint16)
})
save_json_each_row(df_ch_funnel, os.path.join(SEEDS_DIR, "clickhouse", "search_funnel_logs.json"))

start_unix_timestamp = int(base_time.timestamp())
df_ch_scans = pd.DataFrame({
    'log_date': [target_date_str] * num_orders,  
    'event_id': event_ids,
    'package_id': package_ids,
    'order_id': order_ids,
    'product_id': order_product_map,
    'timestamp': (start_unix_timestamp + np.random.randint(300, 86400, size=num_orders)).astype(np.uint32),
    'status': np.random.choice(["WH_RECEIVE",
                                "WH_DISPATCH",
                                "CROSSDOCK_ARRIVE",
                                "CROSSDOCK_DEPART",
                                "HUB_ARRIVE",
                                "HUB_SORT",
                                "PVZ_ARRIVE",
                                "CUSTOMER_PICKED"], size=num_orders),
    'darkstore_id': np.random.choice(darkstore_nodes, size=num_orders),
    'executor_id': np.random.choice(executor_nodes, size=num_orders),
    'executor_type': np.random.choice(executor_types, size=num_orders),
})
save_json_each_row(df_ch_scans, os.path.join(SEEDS_DIR, "clickhouse", "clickstream_scan_events.json"))

print("[3/4] ГЕНЕРАЦИЯ: Elasticsearch Document Layer (Strict Mapping)...")

es_catalog = []
promo_campaign_types = ["BLACK_FRIDAY_2026", "CYBER_MONDAY", "WEEKEND_SALE", "LIQUIDATION"]
discount_types = ["PERCENTAGE", "FIXED_RUB"]

for idx, p_id in enumerate(product_ids):
    has_active_promo = (idx % 8 == 0)
    
    promotions = []
    if has_active_promo:
        promotions.append({
            "promo_campaign_id": str(np.random.choice(promo_campaign_types)),
            "start_date": target_date_str,
            "end_date": (base_time + timedelta(days=7)).strftime("%Y-%m-%d"),
            "status": "ACTIVE",
            "discount_type": str(np.random.choice(discount_types)),
            "discount_value": round(float(np.random.uniform(5.0, 50.0)), 2)
        })
        
    es_document = {
        "product_id": str(p_id),
        "base_price": round(float(np.random.uniform(150.0, 85000.0)), 2),
        "marketing_tags": [str(np.random.choice(["HIT", "SALE", "NEW", "ECO_FRIENDLY"]))],
        "bundle_compatible_ids": [str(np.random.choice(product_ids)) for _ in range(2)],
        "category_hierarchy": {
            "category_id": f"CAT_{np.random.randint(1, 15):03d}",
            "category_name": f"Логистическая Категория Топлива {np.random.randint(1,15)}",
            "level": int(np.random.choice([1, 2, 3]))
        },
        "active_promotions": promotions  
    }
    es_catalog.append(es_document)

with open(os.path.join(SEEDS_DIR, "elasticsearch", "retail_catalog.json"), "w", encoding="utf-8") as f:
    json.dump(es_catalog, f, indent=2, ensure_ascii=False)
    print("\n[SUCCESS] Успешно завершена генерация тестовых данных!")
    print(f"Итог контуров: {num_users} пользователей жестко связаны с {num_orders} транзакциями.")

print("[4/4] ГЕНЕРАЦИЯ: Neo4j Graph Layer (1-я и 2-я подзадачи)...")

neo_origins = []
for i in range(1, 5):
    neo_origins.append({
        "id": f"WH_{i:02d}",
        "label": "Warehouse",
        "open_hour": 0.0 if i % 2 == 0 else 6.0,
        "close_hour": 24.0 if i % 2 == 0 else 23.0,
        "risk": round(float(np.random.uniform(1.0, 1.35)), 2)
    })

for ds_id in darkstore_nodes:
    neo_origins.append({
        "id": str(ds_id),
        "label": "Darkstore",
        "open_hour": 8.0,
        "close_hour": 23.0,
        "risk": round(float(np.random.uniform(1.0, 1.25)), 2)
    })

for i in range(1, 9):
    neo_origins.append({
        "id": f"SEL_{i:03d}",
        "label": "SellerWarehouse",
        "open_hour": 9.0,
        "close_hour": 21.0,
        "risk": round(float(np.random.uniform(1.0, 1.45)), 2)
    })

neo_transit_hubs = [
    {
        "id": f"HUB_{i:03d}",
        "open_hour": float(np.random.choice([0.0, 5.0, 6.0, 7.0])),
        "close_hour": float(np.random.choice([22.0, 23.0, 23.5, 24.0])),
        "risk": round(float(np.random.uniform(1.0, 1.5)), 2)
    }
    for i in range(1, 16)
]

neo_pickup_points = [
    {
        "id": f"PP_{i:03d}",
        "coef": round(float(np.random.uniform(0.05, 0.95)), 2),
        "open_hour": float(np.random.choice([8.0, 9.0, 10.0])),
        "close_hour": float(np.random.choice([21.0, 22.0, 23.0])),
        "current_load_pct": round(float(np.random.uniform(0.15, 0.95)), 2),
        "risk": round(float(np.random.uniform(1.0, 1.2)), 2)
    }
    for i in range(1, 20)
]

neo_corridors = []
neo_legs_map = {}
hub_ids_pool = [h["id"] for h in neo_transit_hubs]

for pp in neo_pickup_points:
    selected_origins = list(np.random.choice(neo_origins, size=min(4, len(neo_origins)), replace=False))
    for orig in selected_origins:
        chain_len = 1 if orig["label"] == "Darkstore" else int(np.random.randint(1, 4))
        chain_ids = [str(x) for x in np.random.choice(hub_ids_pool, size=chain_len, replace=False)]
        times = [round(float(np.random.uniform(0.5, 8.5)), 1) for _ in chain_ids]
        last_mile_time = round(float(np.random.uniform(0.3, 1.5)), 2)

        neo_corridors.append({
            "origin_id": orig["id"],
            "pp_id": pp["id"],
            "chain": chain_ids,
            "times": times,
            "base_time": round(sum(times) + last_mile_time, 2),
            "base_cost": round(float(np.random.uniform(100.0, 950.0)), 2)
        })

        full_path = [orig["id"]] + chain_ids + [pp["id"]]
        step_times = times + [last_mile_time]
        for idx in range(len(full_path) - 1):
            pair = (full_path[idx], full_path[idx + 1])
            if pair not in neo_legs_map:
                neo_legs_map[pair] = {
                    "src_id": pair[0],
                    "dst_id": pair[1],
                    "base_time": step_times[idx],
                    "congestion": round(float(np.random.uniform(1.0, 1.35)), 2),
                    "status": "ACTIVE",
                    "allowed_categories": ["ALL"]
                }

neo_legs = list(neo_legs_map.values())

type_to_label = {
    "WAREHOUSE_WORKER": "WarehouseWorker",
    "LINEHAUL_DRIVER": "LinehaulDriver",
    "LAST_MILE_COURIER": "LastMileCourier",
}
neo_workers_map = {}

for row in df_ch_scans[['executor_id', 'executor_type']].drop_duplicates().to_dict(orient='records'):
    exe_id = str(row['executor_id'])
    w_label = type_to_label.get(str(row['executor_type']), "WarehouseWorker")
    if w_label == "WarehouseWorker":
        props = {"role": str(np.random.choice(["PICKER", "PACKER", "SORTER"])), "shift": str(np.random.choice(["DAY", "NIGHT"]))}
    elif w_label == "LinehaulDriver":
        props = {"vehicle_type": str(np.random.choice(["TRUCK_20T", "TRUCK_10T"])), "shift": "NIGHT"}
    else:
        props = {"transport": str(np.random.choice(["VAN", "BIKE", "FOOT"])), "current_load_orders": int(np.random.randint(1, 25))}

    neo_workers_map[exe_id] = {
        "id": exe_id,
        "label": w_label,
        "props": props,
        "shift_status": "NORMAL",
        "calculated_delay": 0.0
    }

neo_workers = list(neo_workers_map.values())

bottleneck_hubs = hub_ids_pool[:3]  
neo_tracking_events = []

for idx, scan in enumerate(df_ch_scans.to_dict(orient='records')):
    loc_id = str(scan['darkstore_id'])
    ev_ts = int(scan['timestamp'])

    if idx % 10 == 0:
        loc_id = bottleneck_hubs[idx % len(bottleneck_hubs)]
        ev_ts = start_unix_timestamp + 3600 + ((idx % 15) * 240)

    neo_tracking_events.append({
        "user_id": str(order_user_map[idx]),
        "order_id": str(scan['order_id']),
        "package_id": str(scan['package_id']),
        "product_id": str(scan['product_id']),
        "package_type": str(np.random.choice(["STANDARD_BOX", "THERMO_BOX", "FRAGILE_CRATE"])),
        "event_id": str(scan['event_id']),
        "timestamp": ev_ts,
        "status": str(scan['status']),
        "location_id": loc_id,
        "worker_id": str(scan['executor_id'])
    })

neo4j_payload = {
    "customers": [{"id": str(u)} for u in user_ids],
    "origins": neo_origins,
    "transit_hubs": neo_transit_hubs,
    "pickup_points": neo_pickup_points,
    "corridors": neo_corridors,
    "legs": neo_legs,
    "workers": neo_workers,
    "tracking_events": neo_tracking_events
}

with open(os.path.join(SEEDS_DIR, "neo4j", "logistics_graph.json"), "w", encoding="utf-8") as f:
    json.dump(neo4j_payload, f, indent=2, ensure_ascii=False)
