import os
from pathlib import Path
import time
import logging

from pyspark.sql import SparkSession
from data_pipelines.shared.config import GlobalSettings
from .repositories.clickhouse_reader import ClickHouseReader
from .repositories.neo4j_logistics import Neo4jLogisticsRepository
from .repositories.elasticsearch_marketing import ElasticsearchMarketingRepository
from .repositories.postgres_writer import PostgresWriterRepository
from .feature_generator import FrustrationExperienceRisksFeatureGenerator
from .data_quality.rules import DataQualityGate
from pyspark.sql import functions as F

logger = logging.getLogger("RealTimeCommerce.FrustrationExperienceRisks.Pipeline")


class FrustrationExperienceRisksPipeline:

    def __init__(
        self,
        settings: GlobalSettings,
        ch_reader: ClickHouseReader,
        neo4j_repo: Neo4jLogisticsRepository,
        es_repo: ElasticsearchMarketingRepository,
        postgres_repo: PostgresWriterRepository
    ) -> None:
        self.settings = settings
        self.ch_reader = ch_reader
        self.neo4j_repo = neo4j_repo
        self.es_repo = es_repo
        self.postgres_repo = postgres_repo

        self.feature_generator = FrustrationExperienceRisksFeatureGenerator(self.settings)
        self.dq_gate = DataQualityGate(self.settings)

        self.spark = SparkSession.builder \
            .appName("RealTimeCommerce.UX_Operations_Intelligence.FeaturePipeline") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "200") \
            .getOrCreate()

    def _load_dbt_compiled_query(self, model_name: str) -> str:
        data_pipelines_dir = Path(__file__).resolve().parents[2]
        sql_path = (
            data_pipelines_dir
            / "dbt"
            / "target"
            / "compiled"
            / "contextual_commerce_dbt"
            / "models"
            / "staging"
            / f"{model_name}.sql"
        )

        if not sql_path.exists():
            raise FileNotFoundError(
                f"Скомпилированный SQL-файл dbt не найден! Проверьте шаг 'dbt compile'. Путь: {sql_path}"
            )

        return sql_path.read_text(encoding="utf-8")

    def _load_postgres_ux_features_upsert_query(self) -> str:
        sql_path = Path(__file__).resolve().parent / "queries" / "postgres" / "upsert_ux_features.sql"
        if not sql_path.exists():
            raise FileNotFoundError(f"Внешний SQL-шаблон вставки признаков не найден по пути: {sql_path}")
        return sql_path.read_text(encoding="utf-8")

    def _load_postgres_logistics_features_upsert_query(self) -> str:
        sql_path = Path(__file__).resolve().parent / "queries" / "postgres" / "upsert_logistics_features.sql"
        if not sql_path.exists():
            raise FileNotFoundError(f"Внешний SQL-шаблон вставки логистических признаков не найден по пути: {sql_path}")
        return sql_path.read_text(encoding="utf-8")


    def execute_sync(self, target_date: str) -> None:
        logger.info(f"Запуск распределенного Feature-пайплайна первой подзадачи за дату: {target_date}")
        start_time = time.perf_counter()

        try:
            logger.info("Выгрузка параллельных потоков сырых логов из ClickHouse...")
            ch_url = self.ch_reader.get_jdbc_url()
            
            raw_clicks_df = self.spark.read.format("jdbc") \
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
                .option("url", ch_url) \
                .option("query", self._load_dbt_compiled_query("stg_user_clicks")) \
                .load()

            raw_perf_df = self.spark.read.format("jdbc") \
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
                .option("url", ch_url).option("query", self._load_dbt_compiled_query("stg_web_performance")).load()
                
            raw_search_df = self.spark.read.format("jdbc") \
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
                .option("url", ch_url).option("query", self._load_dbt_compiled_query("stg_search_funnel")).load()

            logger.info("Загрузка графового паспорта коридоров из Neo4j и готового JSON из Elasticsearch...")
            neo4j_features_df = self.neo4j_repo.fetch_logistics_spark_df(self.spark)
            es_marketing_df = self.es_repo.fetch_marketing_spark_df(self.spark, target_date)

            self.dq_gate.validate_inputs(
                raw_clicks=raw_clicks_df,
                raw_perf=raw_perf_df,
                raw_search=raw_search_df,
                neo4j_df=neo4j_features_df,
                es_df=es_marketing_df
            )

            logger.info("Сборка матрицы признаков интерфейса силами FeatureGenerator...")
            input_features_df = self.feature_generator.generate_features_matrix(
                raw_clicks_df, 
                raw_perf_df, 
                raw_search_df, 
                neo4j_features_df, 
                es_marketing_df, 
                target_date
            )

            logger.info("Инициализация сброса фичей во внешний буфер TimescaleDB...")
            pg_url, pg_properties = self.postgres_repo.get_jdbc_config()
            ux_features_upsert_statement = self._load_postgres_ux_features_upsert_query()
            logistics_features_upsert_statement = self._load_postgres_logistics_features_upsert_query()

            ux_features_df = input_features_df[
                'log_date',
                'user_id',
                'max_session_depth',
                'p90_frustration',
                'total_rage_clicks',
                'pogo_sticking_count',
                'max_velocity_volatility',
                'max_unique_products',
                'max_unique_categories',
                'total_perf_events',
                'api_errors_count',
                'p95_interface_render_ms',
                'avg_network_ms',
                'total_searches',
                'total_filter_resets',
                'max_search_scroll',
                'origin_node_id',
                'pickup_point_id',
                'promo_price',
                'has_bundles',
                'category_depth'
            ]

            self.postgres_repo.delete_partition_for_date(target_date)

            ux_features_df.write \
                .format("jdbc") \
                .option("url", pg_url) \
                .option("dbtable", "stg_analytics.ux_features_buffer") \
                .option("queryCustom", ux_features_upsert_statement) \
                .options(**pg_properties) \
                .mode("append") \
                .save()

            logistics_features_df = input_features_df.select(
                "log_date",
                "user_id",
                "product_id",
                F.lit("").alias("order_id"),
                "origin_node_id",
                "pickup_point_id",
                "route_time",
                "route_cost",
                "route_jitter",
                "bottleneck_risk_factor",
                "graph_hops",
                "isolation_score",
            )

            logistics_features_df.write \
                .format("jdbc") \
                .option("url", pg_url) \
                .option("dbtable", "stg_analytics.logistics_features_buffer_1") \
                .options(**pg_properties) \
                .mode("append") \
                .save()


            total_duration = time.perf_counter() - start_time
            logger.info(f"[УСПЕХ] Все фичи успешно закоммичены в Feature Store за {total_duration:.2f}с!")

        except Exception as e:
            logger.error(f"Критический сбой Feature-пайплайна первой подзадачи: {str(e)}", exc_info=True)
            raise e
        finally:
            logger.info("Остановка распределенной Spark-сессии...")
            self.spark.stop()
            logger.info("Вычислительные ресурсы воркеров успешно освобождены.")
