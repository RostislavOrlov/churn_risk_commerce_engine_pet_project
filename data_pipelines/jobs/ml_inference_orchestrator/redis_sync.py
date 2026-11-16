import json
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
import redis

from data_pipelines.shared.config import settings
from data_pipelines.shared.connections import infra

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("RealTimeCommerce.OnlineServing.RedisSync")

FEATURE_TTL_SECONDS = 172800   # 48 часов для профиля фичей пользователя
TOPOLOGY_TTL_SECONDS = 86400   # 24 часа для пропускной способности хабов
INCIDENTS_TTL_SECONDS = 86400  # 24 часа для тепловой карты активных сбоев 


def _get_pg_connection():
    if hasattr(infra, "get_postgres_connection"):
        return infra.get_postgres_connection()
    pg_cfg = getattr(settings, "postgres_config", {})
    return psycopg2.connect(
        host=pg_cfg.get("host") or settings.get("POSTGRES_HOST", "postgres-master"),
        port=pg_cfg.get("port") or settings.get("POSTGRES_PORT", 5432),
        dbname=pg_cfg.get("database") or settings.get("POSTGRES_DB", "retail_oltp"),
        user=pg_cfg.get("user") or settings.get("POSTGRES_USER", "retail_user"),
        password=pg_cfg.get("password") or settings.get("POSTGRES_PASSWORD", "retail_password"),
    )


def _get_redis_client() -> redis.Redis:
    if hasattr(infra, "get_redis_client"):
        return infra.get_redis_client()
    redis_cfg = getattr(settings, "redis_config", {})
    return redis.Redis(
        host=redis_cfg.get("host") or settings.get("REDIS_HOST", "redis"),
        port=int(redis_cfg.get("port") or settings.get("REDIS_PORT", 6379)),
        password=redis_cfg.get("password") or settings.get("REDIS_PASSWORD", None),
        db=0,
        decode_responses=True,
    )


def sync_marts_to_redis(**kwargs) -> None:
    logger.info("Запуск загрузки Online Feature Store и оперативных кэшей в Redis...")
    pg_conn = _get_pg_connection()
    r_client = _get_redis_client()

    try:
        r_client.ping()
        pipe = r_client.pipeline(transaction=False)

        with pg_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT 
                    u.user_id,
                    u.max_session_depth,
                    u.p90_frustration,
                    u.total_rage_clicks,
                    u.pogo_sticking_count,
                    u.max_velocity_volatility,
                    u.max_unique_products,
                    u.max_unique_categories,
                    u.total_perf_events,
                    u.api_errors_count,
                    u.p95_interface_render_ms,
                    u.avg_network_ms,
                    u.total_searches,
                    u.total_filter_resets,
                    u.max_search_scroll,
                    u.promo_price,
                    u.has_bundles,
                    u.category_depth AS marketing_tags_count,
                    COALESCE(ltv.long_term_value_rub, 0.0) AS user_ltv,
                    COALESCE(ltv.customer_segment, 'REGULAR') AS customer_segment
                FROM stg_analytics.ux_features_buffer u
                LEFT JOIN stg_analytics.logistics_features_buffer l
                    ON u.log_date = l.log_date AND u.user_id = l.user_id
                LEFT JOIN dw_marts.dwh_user_ltv ltv
                    ON u.user_id = ltv.user_id
                WHERE u.log_date = (SELECT MAX(log_date) FROM stg_analytics.ux_features_buffer);
            """)
            feature_rows = cur.fetchall()
            for row in feature_rows:
                key = f"online:features:user:{row['user_id']}"
                mapped = {k: str(v) for k, v in row.items() if k != "user_id"}
                pipe.hset(key, mapping=mapped)
                pipe.expire(key, FEATURE_TTL_SECONDS)
            pipe.execute()
            logger.info(f"Загружены векторы признаков для CatBoost (Online Feature Store): {len(feature_rows)}")

            cur.execute("""
                SELECT node_id, node_type, base_capacity_packages,
                       current_capacity_packages, bottleneck_risk_factor, is_active_block
                FROM dw_marts.node_dynamic_capacities;
            """)
            node_rows = cur.fetchall()
            blocked_nodes = []

            for row in node_rows:
                node_payload = {
                    "node_id": row["node_id"],
                    "node_type": row["node_type"],
                    "base_capacity_packages": row["base_capacity_packages"],
                    "current_capacity_packages": row["current_capacity_packages"],
                    "bottleneck_risk_factor": float(row["bottleneck_risk_factor"]),
                    "is_active_block": int(row["is_active_block"]),
                }
                key = f"online:node:capacity:{row['node_id']}"
                pipe.hset(key, mapping={k: str(v) for k, v in node_payload.items()})
                pipe.expire(key, TOPOLOGY_TTL_SECONDS)

                if node_payload["is_active_block"] == 1 or node_payload["bottleneck_risk_factor"] > 0.5:
                    blocked_nodes.append(node_payload)

            pipe.set("online:topology:blocked_nodes", json.dumps(blocked_nodes, ensure_ascii=False), ex=TOPOLOGY_TTL_SECONDS)
            pipe.execute()
            logger.info(f"Обновлено хабов: {len(node_rows)} (из них проблемных/заблокированных: {len(blocked_nodes)})")

            cur.execute("""
                SELECT order_id, failing_node_id, logistic_stage_idx,
                       calculated_delay_minutes, incident_status, assigned_team
                FROM dw_marts.incident_log
                WHERE incident_status IN ('RAISED', 'REROUTED')
                ORDER BY calculated_delay_minutes DESC
                LIMIT 500;
            """)
            incident_rows = [dict(r) for r in cur.fetchall()]
            for inc in incident_rows:
                inc["calculated_delay_minutes"] = float(inc["calculated_delay_minutes"])

            pipe.set("online:incidents:active", json.dumps(incident_rows, ensure_ascii=False), ex=INCIDENTS_TTL_SECONDS)
            pipe.execute()
            logger.info(f"Кэш активных инцидентов Ситуационного центра обновлен: {len(incident_rows)} записей")

        logger.info("Синхронизация Online Serving Layer в Redis полностью завершена!")

    finally:
        pg_conn.close()
        r_client.close()


if __name__ == "__main__":
    sync_marts_to_redis()
