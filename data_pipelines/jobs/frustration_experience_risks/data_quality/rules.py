import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger("RealTimeCommerce.FrustrationExperienceRisks.DataQuality")


class DataQualityGate:

    def __init__(self, settings: dict = None) -> None:
        self.settings = settings or {}

    def validate_inputs(
        self,
        raw_clicks: DataFrame,
        raw_perf: DataFrame,
        raw_search: DataFrame,
        neo4j_df: DataFrame,
        es_df: DataFrame
    ) -> None:
        logger.info("[DQ GATE] Запуск расширенного контроля качества данных...")

        clicks_count = raw_clicks.count()
        logger.info(f"[DQ] Объем сырого кликстрима ClickHouse: {clicks_count} строк.")
        if clicks_count == 0:
            raise ValueError("[DQ CRITICAL] Таблица сырых кликов ClickHouse пуста за целевые сутки!")

        neo_count = neo4j_df.count()
        logger.info(f"[DQ] Объем графового паспорта Neo4j: {neo_count} строк.")
        if neo_count == 0:
            raise ValueError("[DQ CRITICAL] Графовый паспорт Neo4j вернул 0 строк! Сбой проекции GDS.")

        total_perf = raw_perf.count()
        unique_perf = raw_perf.select("user_id", "status_code", "page_load_ms").distinct().count() 
        
        logger.info(f"[DQ] Проверка дубликатов телеметрии: всего {total_perf}, уникальных структур {unique_perf}")
        if (total_perf - unique_perf) / total_perf * 100 > 2.0:
            raise ValueError("[DQ CRITICAL] Обнаружено более 2% дубликатов в логах производительности! Риск задвоения метрик.")

        es_products = es_df.select(F.col("_source.product_id").alias("product_id")).distinct()
        click_products = raw_clicks.select("product_id").distinct()
        
        broken_products_count = click_products.join(es_products, "product_id", "left_anti").count()
        total_products_count = click_products.count()
        
        broken_pct = (broken_products_count / total_products_count * 100) if total_products_count > 0 else 0
        logger.info(f"[DQ] Проверка синхронизации баз: {broken_pct:.2f}% продуктов из логов отсутствуют в Elasticsearch.")
        if broken_pct > 10.0:
            raise ValueError(f"[DQ CRITICAL] Рассинхронизация ID: {broken_pct:.2f}% товаров из кликстрима нет в каталоге ES! (Лимит: 10%)")

        stats = raw_perf.select(
            F.avg("page_load_ms").alias("avg_load"),
            F.stddev_pop("page_load_ms").alias("std_load")
        ).collect()[0]
        
        avg_load = float(stats["avg_load"]) if stats["avg_load"] is not None else 0.0
        std_load = float(stats["std_load"]) if stats["std_load"] is not None else 1.0
        
        anomaly_threshold = avg_load + (4.0 * std_load)

        logger.info(f"[DQ] Статистика рендеринга: среднее={avg_load:.2f}мс, порог аномалии (4-Sigma)={anomaly_threshold:.2f}мс")
        
        anomalies_count = raw_perf.filter(F.col("page_load_ms") > anomaly_threshold).count()
        anomalies_pct = (anomalies_count / total_perf * 100) if total_perf > 0 else 0

        logger.info(f"[DQ] Обнаружено {anomalies_count} выбросов рендеринга ({anomalies_pct:.2f}% от всех логов).")
        if anomalies_pct > 5.0:
            raise ValueError(f"[DQ CRITICAL] Инфраструктурный коллапс: более 5% логов превысили порог 4-Sigma ({anomaly_threshold:.2f}мс). Логи загрязнены аномальными таймингами.")

        invalid_users_pct = raw_clicks.filter(
            F.col("user_id").isNull() | (F.col("user_id") == "0") | (F.col("user_id") == "")
        ).count() / clicks_count * 100

        logger.info(f"[DQ] Доля пустых user_id в кликстриме: {invalid_users_pct:.2f}%")
        if invalid_users_pct > 5.0:
            raise ValueError(f"[DQ CRITICAL] Превышен лимит пустых user_id: {invalid_users_pct:.2f}% (Лимит: 5.0%)")

        logger.info("[DQ SUCCESS] Все 5 этапов распределенного контроля успешно пройдены! Матрица фичей защищена.")
