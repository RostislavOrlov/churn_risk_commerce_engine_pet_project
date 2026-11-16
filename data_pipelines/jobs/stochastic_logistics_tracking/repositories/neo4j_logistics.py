import time
import random
import logging
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession, DataFrame
from data_pipelines.shared.config import GlobalSettings, settings

logger = logging.getLogger("RealTimeCommerce.StochasticLogisticsTracking.Neo4jGDS")


class Neo4jLogisticsRepository:

    def __init__(self, settings: GlobalSettings = settings, neo4j_driver: Any = None) -> None:
        self.settings = settings
        self.driver = neo4j_driver
        self.query_project = self._load_query("gds_project.cypher")
        self.query_leiden_bottlenecks = self._load_query("gds_leiden_bottlenecks.cypher")
        self.query_stoch_route = self._load_query("stoch_route_recalc.cypher")
        self.query_drop = self._load_query("gds_drop.cypher")

    def _load_query(self, file_name: str) -> str:
        base_dir = Path(__file__).resolve().parent.parent
        path = base_dir / "queries" / "neo4j" / file_name
        lines = [
            line.split("//")[0].strip()
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.split("//")[0].strip()
        ]
        return "\n".join(lines).rstrip(";")

    def _get_neo4j_bolt_url(self) -> str:
        url = getattr(self.settings, "neo4j_url", None) or getattr(self.settings, "NEO4J_URL", None)
        if url:
            return url.replace("localhost", "neo4j").replace("127.0.0.1", "neo4j")

        host = self.settings.get("NEO4J_HOST", "neo4j")
        if host in ("localhost", "127.0.0.1"):
            host = "neo4j"
        port = self.settings.get("NEO4J_BOLT_PORT", 7687)
        return f"bolt://{host}:{port}"

    def fetch_stochastic_delays_spark_df(self, spark: SparkSession, target_date_or_ids: Any = None) -> DataFrame:
        
        session_graph_id = f"stochastic_highway_batch_{int(time.time())}_{random.randint(1000, 9999)}"
        logger.info(f"Инициализация распределенного GDS-расчета. Graph ID: {session_graph_id}")

        bolt_url = self._get_neo4j_bolt_url()
        user = self.settings.get("NEO4J_USER", "neo4j")
        password = self.settings.get("NEO4J_PASSWORD", "secure_graph_pass_99")

        try:
            with self.driver.session() as session:
                logger.info(f"Создание ин-мемори проекции графа {session_graph_id}...")
                session.run(self.query_project, {"graph_id": session_graph_id}).consume()

                logger.info("Запуск распределенного алгоритма Leiden для локализации тромбов хабов...")
                session.run(self.query_leiden_bottlenecks, {"graph_id": session_graph_id}).consume()

            logger.info("Параллельное сканирование паспортов задержек силами Spark-воркеров...")
            gds_df = (
                spark.read.format("org.neo4j.spark.DataSource")
                .option("url", bolt_url)
                .option("authentication.type", "basic")
                .option("authentication.basic.username", user)
                .option("authentication.basic.password", password)
                .option("partitions", "1")
                .option("batch.size", "25000")
                .option("query", self.query_stoch_route)
                .load()
            )

            gds_cached_df = gds_df.cache()
            rows_count = gds_cached_df.count()

            logger.info(f"Паспорта задержек успешно материализованы в Spark. Строк: {rows_count}")
            return gds_cached_df

        except Exception as e:
            logger.error(f"Критический сбой в распределенном GDS-контуре Neo4j: {str(e)}", exc_info=True)
            return spark.createDataFrame(
                [],
                schema="order_id STRING, calculated_delay_minutes FLOAT, current_logistic_stage INT, is_bottleneck_flag INT, failing_node_id STRING",
            )

        finally:
            logger.info(f"Выжигание ин-мемори проекции {session_graph_id} из ОЗУ Neo4j...")
            try:
                with self.driver.session() as session:
                    session.run(self.query_drop, {"graph_id": session_graph_id}).consume()
                logger.info(f"Ин-мемори граф {session_graph_id} успешно удален из RAM.")
            except Exception as drop_error:
                logger.error(f"Не удалось выжечь граф {session_graph_id}: {str(drop_error)}")
