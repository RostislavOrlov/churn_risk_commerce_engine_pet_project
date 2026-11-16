import logging
from pathlib import Path
from typing import Any
from pyspark.sql import DataFrame
from data_pipelines.shared.config import GlobalSettings, settings
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


logger = logging.getLogger("RealTimeCommerce.LogisticsGraphRefresh.Neo4jBulkImporter")


class Neo4jBulkImporterRepository:

    def __init__(self, settings: GlobalSettings = settings, neo4j_driver: Any = None) -> None:
        self.settings = settings
        self.driver = neo4j_driver
        self.bulk_cypher = self._load_query("gds_bulk_import.cypher")

    def _load_query(self, file_name: str) -> str:
        base_dir = Path(__file__).resolve().parent.parent
        path = base_dir / "queries" / "neo4j" / file_name

        if not path.exists():
            raise FileNotFoundError(f"Внешний Cypher-шаблон импорта не найден по пути: {path}")

        lines = [
            line.split("//")[0].strip()
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.split("//")[0].strip()
        ]
        return "\n".join(lines).rstrip(";")

    def _get_bolt_url(self) -> str:
        url = getattr(self.settings, "neo4j_url", None) or getattr(self.settings, "NEO4J_URL", None)
        if url:
            return url.replace("localhost", "neo4j").replace("127.0.0.1", "neo4j")

        host = self.settings.get("NEO4J_HOST", "neo4j")
        if host in ("localhost", "127.0.0.1"):
            host = "neo4j"
        port = self.settings.get("NEO4J_BOLT_PORT", 7687)
        return f"bolt://{host}:{port}"

    def import_logistics_spark_df(self, raw_scans_df: DataFrame) -> None:
        logger.info("Запуск высокоскоростного распределенного импорта ScanEvent в транзакционный слой Neo4j...")

        bolt_url = self._get_bolt_url()
        user = self.settings.get("NEO4J_USER", "neo4j")
        password = self.settings.get("NEO4J_PASSWORD", "secure_graph_pass_99")

        clean_df = raw_scans_df
        for field in clean_df.schema.fields:
            if isinstance(field.dataType, DecimalType):
                target_type = "long" if field.dataType.scale == 0 else "double"
                clean_df = clean_df.withColumn(field.name, F.col(field.name).cast(target_type))

        clean_df.coalesce(2).write \
            .format("org.neo4j.spark.DataSource") \
            .option("url", bolt_url) \
            .option("authentication.type", "basic") \
            .option("authentication.basic.username", user) \
            .option("authentication.basic.password", password) \
            .option("query", self.bulk_cypher) \
            .option("batch.size", "5000") \
            .mode("Append") \
            .save()

        logger.info("Распределенный Bulk Insert ребер и узлов логов сканирований в Neo4j успешно завершен.")
