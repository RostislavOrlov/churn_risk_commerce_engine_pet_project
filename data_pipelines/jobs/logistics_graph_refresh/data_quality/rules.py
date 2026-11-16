import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger("RealTimeCommerce.StochasticLogisticsTracking.DataQuality")


class LogisticsDataQualityGate:

    def __init__(self, settings: dict = None) -> None:
        self.settings = settings or {}

    def validate_inputs(self, raw_scans: DataFrame) -> None:
        logger.info("Запуск распределенного контроля качества ScanEvent...")

        scans_count = raw_scans.count()
        logger.info(f"[DQ] Объем суточного инкремента сканирований ClickHouse: {scans_count} строк.")
        if scans_count == 0:
            raise ValueError("[DQ CRITICAL] Сбой логов: инкремент clickstream_scan_events пуст за целевые сутки!")

        unique_scans_count = raw_scans.select("event_id").distinct().count()
        duplicate_pct = ((scans_count - unique_scans_count) / scans_count) * 100
        
        logger.info(f"[DQ] Проверка дубликатов: уникальных GUID={unique_scans_count}, доля дублей={duplicate_pct:.2f}%")
        if duplicate_pct > 1.0:
            raise ValueError(f"[DQ CRITICAL] Задвоение трафика: обнаружено {duplicate_pct:.2f}% дубликатов GUID в ScanEvent! (Лимит: 1.0%)")

        package_window = Window.partitionBy("package_id").orderBy("timestamp")
        
        temporal_anomalies_df = raw_scans \
            .withColumn("prev_timestamp", F.lag("timestamp", 1).over(package_window)) \
            .filter(F.col("prev_timestamp").isNotNull() & (F.col("timestamp") < F.col("prev_timestamp")))
            
        anomalies_count = temporal_anomalies_df.count()
        anomalies_pct = (anomalies_count / scans_count) * 100
        
        logger.info(f"[DQ] Хронологический аудит: выявлено {anomalies_count} аномалий трека курьеров/магистралей ({anomalies_pct:.2f}% от батча).")
        if anomalies_pct > 3.0:
            raise ValueError(f"[DQ CRITICAL] Хронологический коллапс: более 3.0% посылок имеют рассинхронизацию времени чекпоинтов ({anomalies_pct:.2f}%). Ошибка часов ТСД или транспортных логов.")

        logger.info("Все этапы контроля успешно пройдены! Зеленый коридор для Neo4j Bulk Importer.")
