import time
import logging
from pathlib import Path
from pyspark.sql import SparkSession

from data_pipelines.shared.config import GlobalSettings
from data_pipelines.jobs.logistics_graph_refresh.repositories.clickhouse_event_stream import ClickHouseEventStreamRepository
from data_pipelines.jobs.logistics_graph_refresh.repositories.neo4j_bulk_importer import Neo4jBulkImporterRepository
from data_pipelines.jobs.logistics_graph_refresh.data_quality.rules import LogisticsDataQualityGate

logger = logging.getLogger("RealTimeCommerce.IngestLogisticsStream.Pipeline")


class IngestLogisticsStreamPipeline:

    def __init__(
        self,
        settings: GlobalSettings,
        ch_stream_repo: ClickHouseEventStreamRepository,
        neo4j_bulk_repo: Neo4jBulkImporterRepository,
    ) -> None:
        self.settings = settings
        self.ch_stream_repo = ch_stream_repo
        self.neo4j_bulk_repo = neo4j_bulk_repo
        self.dq_gate = LogisticsDataQualityGate(self.settings)

        self.spark = (
            SparkSession.builder.appName("RealTimeCommerce.IngestLogisticsStream.Pipeline")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate()
        )

    def _load_clickhouse_query(self, target_date: str) -> str:
        file_path = (
            Path(__file__).resolve().parent
            / "queries"
            / "clickhouse"
            / "fetch_scan_events_stream.sql"
        )

        logger.info("Считывание SQL-шаблона инкремента ClickHouse по пути: %s", file_path)
        if not file_path.exists():
            raise FileNotFoundError(
                f"Критический SQL-шаблон выгрузки ClickHouse не найден по пути: {file_path}"
            )

        raw_sql = file_path.read_text(encoding="utf-8").strip().rstrip(";")
        return (
            raw_sql
            .replace("%(date)s", f"'{target_date}'")
            .replace("{target_date}", f"'{target_date}'")
        )

    def execute_ingest_sync(self, target_date: str) -> None:
        logger.info("Запуск распределенного Ingestion-пайплайна за дату: %s", target_date)
        start_time = time.perf_counter()

        try:
            ch_jdbc_url = self.ch_stream_repo.get_jdbc_url()
            optimized_query = self._load_clickhouse_query(target_date)

            logger.info("Инициализация JDBC-выгрузки воркерами Spark логов из ClickHouse...")
            scan_events_df = (
                self.spark.read.format("jdbc")
                .option("url", ch_jdbc_url)
                .option("dbtable", f"({optimized_query}) AS incremental_log")
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
                .load()
            )

            logger.info("Проверка датафрейма на соответствие правилам Data Quality Gate...")
            self.dq_gate.validate_inputs(scan_events_df)

            logger.info("Запуск Spark Bulk-Importer. Формирование узлов и протягивание ребер в Neo4j...")
            self.neo4j_bulk_repo.import_logistics_spark_df(scan_events_df)

            duration = time.perf_counter() - start_time
            logger.info(
                "[УСПЕХ] Граф логистики Neo4j успешно прогрет отвалидированным инкрементом за %.2fс",
                duration,
            )

        except Exception as err:
            logger.error(
                "Критический сбой Ingestion-пайплайна импорта логистики: %s",
                str(err),
                exc_info=True,
            )
            raise err

        finally:
            logger.info("Остановка Spark-сессии Ingestion-контура и освобождение дескрипторов...")
            self.spark.stop()
            logger.info("Распределенный Ingestion-процесс полностью завершен")
