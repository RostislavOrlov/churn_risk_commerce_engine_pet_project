import logging
import psycopg2
from typing import Any, Tuple, Dict
from data_pipelines.shared.config import GlobalSettings, settings
from data_pipelines.shared.connections import infra

logger = logging.getLogger("RealTimeCommerce.StochasticLogisticsTracking.PostgresWriter")


class PostgresWriterRepository:

    def __init__(self, settings: GlobalSettings = settings, pool_provider: Any = None) -> None:
        self.settings = settings
        self.pool = pool_provider or infra

    def get_jdbc_config(self) -> Tuple[str, Dict[str, str]]:
        pg_cfg = getattr(self.settings, "postgres_config", {})
        host = pg_cfg.get("host") or self.settings.get("POSTGRES_HOST", "postgres-master")
        port = pg_cfg.get("port") or self.settings.get("POSTGRES_PORT", 5432)
        database = pg_cfg.get("database") or self.settings.get("POSTGRES_DB", "retail_oltp")
        user = pg_cfg.get("user") or self.settings.get("POSTGRES_USER", "retail_user")
        password = pg_cfg.get("password") or self.settings.get("POSTGRES_PASSWORD", "retail_password")

        jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"
        jdbc_properties = {
            "user": str(user),
            "password": str(password),
            "driver": "org.postgresql.Driver",
            "batchsize": "10000",
            "reWriteBatchedInserts": "true",
        }

        logger.info(f"Сформирована PostgreSQL JDBC-конфигурация для Spark: {host}:{port}/{database}")
        return jdbc_url, jdbc_properties

    def _acquire_pg_connection(self):
        if hasattr(self.pool, "get_postgres_connection"):
            return self.pool.get_postgres_connection(), False
        if hasattr(self.pool, "get_connection"):
            return self.pool.get_connection(), True

        pg_cfg = getattr(self.settings, "postgres_config", {})
        conn = psycopg2.connect(
            host=pg_cfg.get("host") or self.settings.get("POSTGRES_HOST", "postgres-master"),
            port=pg_cfg.get("port") or self.settings.get("POSTGRES_PORT", 5432),
            dbname=pg_cfg.get("database") or self.settings.get("POSTGRES_DB", "retail_oltp"),
            user=pg_cfg.get("user") or self.settings.get("POSTGRES_USER", "retail_user"),
            password=pg_cfg.get("password") or self.settings.get("POSTGRES_PASSWORD", "retail_password"),
        )
        return conn, False

    def delete_partition_for_date(self, target_date: str) -> None:
        conn, is_pooled = self._acquire_pg_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM stg_analytics.logistics_features_buffer_2 WHERE log_date = %s::DATE;",
                    (target_date,),
                )
            conn.commit()
            logger.info(f"Буфер stg_analytics.logistics_features_buffer_2 за {target_date} очищен.")
        except Exception as e:
            conn.rollback()
            logger.error(f"Ошибка очистки буфера за {target_date}: {e}")
            raise
        finally:
            if is_pooled and hasattr(self.pool, "release_connection"):
                self.pool.release_connection(conn)
            else:
                conn.close()
