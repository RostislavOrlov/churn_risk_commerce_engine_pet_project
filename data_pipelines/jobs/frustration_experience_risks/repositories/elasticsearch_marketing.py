import os
import json
import logging
from string import Template
from typing import Any

from pyspark.sql import SparkSession, DataFrame
from data_pipelines.shared.config import GlobalSettings

logger = logging.getLogger("RealTimeCommerce.ElasticsearchMarketing")


class ElasticsearchMarketingRepository:

    def __init__(self, settings: GlobalSettings, es_client: Any) -> None:
        self.settings = settings
        self.client = es_client
        self.query_template = self._load_json_template()

    def _load_json_template(self) -> Template:
        base_dir = os.path.dirname(os.path.dirname(__file__))
        path = os.path.join(base_dir, "queries", "elasticsearch", "marketing_catalog.json")
        with open(path, "r", encoding="utf-8") as file:
            return Template(file.read())

    def fetch_marketing_spark_df(self, spark: SparkSession, target_date: str) -> DataFrame:
        
        logger.info(f"Рендеринг внешнего DSL-запроса Elasticsearch на дату: {target_date}")

        rendered_query_str = self.query_template.substitute(
            target_product_ids="[]", 
            target_date=target_date,
            size="10000" 
        )
        
        advanced_es_query = json.loads(rendered_query_str)
        
        if "must" in advanced_es_query["query"]["bool"] and len(advanced_es_query["query"]["bool"]["must"]) > 0:
            advanced_es_query["query"]["bool"]["must"] = [{"match_all": {}}]

        es_host = self.settings.get("ELASTICSEARCH_HOST", "elasticsearch")
        es_port = self.settings.get("ELASTICSEARCH_PORT", 9200)

        try:
            marketing_df = spark.read \
                .format("org.elasticsearch.spark.sql") \
                .option("es.nodes", es_host) \
                .option("es.port", str(es_port)) \
                .option("es.query", json.dumps(advanced_es_query)) \
                .option("es.read.metadata", "true") \
                .load("retail_catalog_v2")
                
            logger.info("Маркетинговый каталог успешно подключен к сессии Spark через внешний DSL-шаблон.")
            return marketing_df

        except Exception as e:
            logger.warning(
                f"Нативный коннектор ES недоступен ({str(e)}). "
                f"Переключение на резервный HTTP-скроллинг внешнего шаблона..."
            )
            try:
                res = self.client.search(
                    index="retail_catalog_v2", 
                    body=advanced_es_query,
                    scroll="1m"
                )
                
                scroll_id = res.get("_scroll_id")
                hits = res["hits"]["hits"]
                all_docs = list(hits)

                while scroll_id and len(hits) > 0:
                    res = self.client.scroll(scroll_id=scroll_id, scroll="1m")
                    scroll_id = res.get("_scroll_id")
                    hits = res["hits"]["hits"]
                    all_docs.extend(hits)
                
                if scroll_id:
                    self.client.clear_scroll(scroll_id=scroll_id)

                json_strings = [json.dumps(doc) for doc in all_docs]
                json_rdd = spark.sparkContext.parallelize(json_strings)
                backup_df = spark.read.json(json_rdd)
                
                logger.info(f"Внешний DSL-шаблон успешно обработал {backup_df.count()} промо-документов через HTTP-скролл.")
                return backup_df

            except Exception as critical_es_error:
                logger.error(f"Краш при обработке внешнего JSON-шаблона ES: {str(critical_es_error)}", exc_info=True)
                return spark.createDataFrame([], schema="`_source` STRUCT<product_id: STRING, base_price: DOUBLE, bundle_compatible_ids: ARRAY<STRING>, category_hierarchy: ARRAY<STRUCT<level: INT, category_id: STRING, category_name: STRING>>>, `script_fields` STRUCT<dynamic_promo_price: DOUBLE>")
