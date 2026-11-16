import io
import logging
import pandas as pd
from typing import Any, Tuple, Dict
from data_pipelines.shared.config import GlobalSettings
import psycopg2

logger = logging.getLogger("RealTimeCommerce.PostgresWriter")


class PostgresWriterRepository:

    def __init__(self, settings: GlobalSettings, pool_provider: Any = None):
        self.settings = settings
        self.pool = pool_provider

    def get_jdbc_config(self) -> Tuple[str, Dict[str, str]]:
        host = self.settings.get("POSTGRES_HOST", "postgres-master")
        port = self.settings.get("POSTGRES_PORT", 5432)
        database = self.settings.get("POSTGRES_DB", "retail_oltp")
        user = self.settings.get("POSTGRES_USER", "retail_user")
        password = self.settings.get("POSTGRES_PASSWORD", "retail_password")

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

    def truncate_staging_table(self) -> None:
        logger.info("Выполнение атомарной очистки стейджинга Postgres перед стартом пайплайна...")
        gp_conn = self.pool.get_connection()
        try:
            with gp_conn.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE stg_analytics.stg_ml_user_churn_scores;")
                gp_conn.commit()
                logger.info("Стейджинг-таблица stg_ml_user_churn_scores успешно очищена.")
        except Exception as e:
            gp_conn.rollback()
            logger.error(f"Критический сбой этапа TRUNCATE в Postgres: {str(e)}")
            raise e
        finally:
            self.pool.release_connection(gp_conn)

    def stream_copy_to_staging(self, df: pd.DataFrame, target_date: str) -> None:
        df_stg = pd.DataFrame(index=df.index)
        df_stg["user_id"] = df["user_id"].astype(str)
        df_stg["churn_probability"] = df["churn_probability"].astype(float)
        df_stg["log_date"] = target_date

        output_buffer = io.StringIO()
        df_stg.to_csv(output_buffer, index=False, header=False, sep="\t")
        output_buffer.seek(0)

        gp_conn = self.pool.get_connection()
        try:
            with gp_conn.cursor() as cursor:
                cursor.execute("SET gp_autostats_mode = 'NONE';")
                cursor.copy_from(
                    output_buffer,
                    table="stg_analytics.stg_ml_user_churn_scores",
                    sep="\t",
                    columns=("user_id", "churn_probability", "log_date"),
                )
                gp_conn.commit()
                logger.info(f"Стриминг завершен. Вброшено {len(df_stg)} строк в Postgres Staging.")
        except Exception as e:
            gp_conn.rollback()
            logger.error(f"Критическая ошибка записи COPY в Postgres: {str(e)}")
            raise e
        finally:
            self.pool.release_connection(gp_conn)
            output_buffer.close()
            del df_stg

    def delete_partition_for_date(self, target_date: str) -> None:
        host = self.settings.get("POSTGRES_HOST", "postgres-master")
        port = self.settings.get("POSTGRES_PORT", 5432)
        database = self.settings.get("POSTGRES_DB", "retail_oltp")
        user = self.settings.get("POSTGRES_USER", "retail_user")
        password = self.settings.get("POSTGRES_PASSWORD", "retail_password")

        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=database,
            user=user,
            password=password,
        )
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM stg_analytics.ux_features_buffer WHERE log_date = %s::DATE;",
                    (target_date,),
                )
                cursor.execute(
                    "DELETE FROM stg_analytics.logistics_features_buffer_1 WHERE log_date = %s::DATE;",
                    (target_date,),
                )
            conn.commit()
            logger.info(f"Идемпотентная очистка буферов Postgres за {target_date} успешно выполнена.")
        except Exception as e:
            conn.rollback()
            logger.error(f"Ошибка при очистке буферов за {target_date}: {e}")
            raise
        finally:
            conn.close()
