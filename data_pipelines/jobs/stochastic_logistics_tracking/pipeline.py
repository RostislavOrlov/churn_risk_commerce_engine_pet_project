import time
import logging
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from data_pipelines.shared.config import GlobalSettings
from data_pipelines.jobs.stochastic_logistics_tracking.repositories.neo4j_logistics import Neo4jLogisticsRepository
from data_pipelines.jobs.stochastic_logistics_tracking.repositories.postgres_writer import PostgresWriterRepository
from data_pipelines.jobs.stochastic_logistics_tracking.feature_generator import StochasticLogisticsFeatureGenerator
from data_pipelines.jobs.stochastic_logistics_tracking.data_quality.rules import LogisticsDataQualityGate

logger = logging.getLogger("RealTimeCommerce.StochasticLogisticsTracking.Pipeline")


class StochasticLogisticsTrackingPipeline:

    def __init__(
        self,
        settings: GlobalSettings,
        neo4j_gds_repo: Neo4jLogisticsRepository,
        postgres_repo: PostgresWriterRepository,
        **kwargs: Any,
    ) -> None:
        self.settings = settings
        self.neo4j_gds_repo = neo4j_gds_repo
        self.postgres_repo = postgres_repo

        self.feature_generator = StochasticLogisticsFeatureGenerator(self.settings)
        self.dq_gate = LogisticsDataQualityGate(self.settings)

        self.spark = (
            SparkSession.builder.appName("RealTimeCommerce.StochasticLogisticsTracking.ComputePipeline")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate()
        )

    def _load_postgres_upsert_query(self) -> str:
        file_path = (
            Path(__file__).resolve().parent
            / "queries"
            / "postgres"
            / "upsert_logistics_features.sql"
        )
        if not file_path.exists():
            raise FileNotFoundError(f"Внешний SQL-шаблон вставки признаков логистики не найден по пути: {file_path}")

        return file_path.read_text(encoding="utf-8").strip().rstrip(";")

    def execute_sync(self, target_date: str) -> None:
        logger.info("Запуск распределенного графового Compute-пайплайна за дату: %s", target_date)
        start_time = time.perf_counter()

        try:
            logger.info("Активация GDS аналитического ядра: разворачивание RAM-проекции и расчет коридоров...")
            gds_start_time = time.perf_counter()

            neo4j_gds_df = self.neo4j_gds_repo.fetch_stochastic_delays_spark_df(self.spark, target_date)
            gds_duration = time.perf_counter() - gds_start_time

            logger.info("Активация распределенного барьера DQ Gate для проверки извлеченного графового датафрейма...")
            self.dq_gate.validate_inputs(neo4j_gds_df)

            logger.info("Запуск FeatureGenerator: наполнение мета-колонок и COALESCE-зачистка от пропусков...")
            input_features_df = self.feature_generator.generate_logistics_features(
                self.spark,
                neo4j_gds_df,
                target_date,
            )

            logger.info("Инициализация распределенного сброса логистических фичей во внешний буфер TimescaleDB...")
            pg_url, pg_properties = self.postgres_repo.get_jdbc_config()

            if hasattr(self.postgres_repo, "delete_partition_for_date"):
                self.postgres_repo.delete_partition_for_date(target_date)

            input_features_df = input_features_df.withColumn("log_date", F.to_date(F.lit(target_date)))

            input_features_df.write.format("jdbc").option("url", pg_url).option(
                "dbtable", "stg_analytics.logistics_features_buffer_2"
            ).options(**pg_properties).mode("append").save()

            total_duration = time.perf_counter() - start_time
            logger.info(
                "[УСПЕХ] Вычисления GDS и DQ-валидация успешно завершены за %.2fс, Ожидание графа: %.2fс",
                total_duration,
                gds_duration,
            )

        except Exception as e:
            logger.error("Критический сбой Compute-пайплайна логистики: %s", str(e), exc_info=True)
            raise e
        finally:
            logger.info("Остановка распределенной Spark-сессии...")
            self.spark.stop()
            logger.info("Сквозной распределенный логистический процесс полностью завершен!")
