import os
import time
import random
import logging
from typing import Any

from pyspark.sql import SparkSession, DataFrame
from data_pipelines.shared.config import GlobalSettings

logger = logging.getLogger("RealTimeCommerce.Neo4jLogistics")


class Neo4jLogisticsRepository:

    def __init__(self, settings: GlobalSettings, neo4j_driver: Any) -> None:
        self.settings = settings
        self.driver = neo4j_driver
        
        self.query_project = self._load_query("gds_project.cypher")
        self.query_simulate = self._load_query("logistics.cypher")
        self.query_drop = self._load_query("gds_drop.cypher")

    def _load_query(self, file_name: str) -> str:
        base_dir = os.path.dirname(os.path.dirname(__file__))
        path = os.path.join(base_dir, "queries", "neo4j", file_name)
        with open(path, "r", encoding="utf-8") as file:
            return file.read().strip()

    def _get_neo4j_bolt_url(self) -> str:
        host = self.settings.get("NEO4J_HOST", "neo4j")
        port = self.settings.get("NEO4J_BOLT_PORT", 7687)
        return f"bolt://{host}:{port}"

    def fetch_logistics_spark_df(self, spark: SparkSession) -> DataFrame:
        session_graph_id = f"temporal_batch_logistics_{int(time.time())}_{random.randint(1000, 9999)}"
        logger.info(f"Инициализация GDS-расчета на кластере Neo4j. Сессионный Graph ID: {session_graph_id}")

        bolt_url = self._get_neo4j_bolt_url()
        user = self.settings.get("NEO4J_USER", "neo4j")
        password = self.settings.get("NEO4J_PASSWORD", "secure_graph_pass_99")

        try:
            with self.driver.session() as session:
                logger.info(f"Создание ин-мемори проекции графа {session_graph_id}...")
                session.run(self.query_project, {"graph_id": session_graph_id}).consume()

            logger.info("Подключение Spark-сессии к Bolt-интерфейсу Neo4j для параллельного извлечения...")
            
            logistics_df = spark.read \
                .format("org.neo4j.spark.DataSource") \
                .option("url", bolt_url) \
                .option("authentication.type", "basic") \
                .option("authentication.basic.username", user) \
                .option("authentication.basic.password", password) \
                .option("partitions", "4") \
                .option("batch.size", "25000") \
                .option("query", self.query_simulate) \
                .option("query.parameters", f"graph_id: '{session_graph_id}'") \
                .load()

            logistics_cached_df = logistics_df.cache()
            rows_count = logistics_cached_df.count() 
            
            logger.info(f"Данные успешно извлечены в Spark-кластер. Количество строк: {rows_count}")
            return logistics_cached_df

        except Exception as e:
            logger.error(f"Критический сбой в распределенном GDS-контуре Neo4j: {str(e)}", exc_info=True)
            return spark.createDataFrame([], schema="warehouse_id LONG, pickup_point_id LONG, route_time FLOAT, route_cost FLOAT, route_jitter FLOAT, bottleneck_risk_factor FLOAT, graph_hops INT, isolation_score FLOAT")

        finally:
            logger.info(f"Очистка RAM: удаление ин-мемори графа {session_graph_id} из кластера Neo4j...")
            try:
                with self.driver.session() as session:
                    session.run(self.query_drop, {"graph_id": session_graph_id}).consume()
                logger.info(f"Ин-мемори граф {session_graph_id} успешно выгружен из ОЗУ.")
            except Exception as drop_error:
                logger.error(f"Не удалось удалить граф {session_graph_id} из RAM: {str(drop_error)}")
