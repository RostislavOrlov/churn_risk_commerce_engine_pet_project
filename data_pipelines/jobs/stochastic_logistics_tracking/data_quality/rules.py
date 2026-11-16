import logging
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from data_pipelines.shared.config import GlobalSettings
from ..core.exceptions import DataQualityViolationException

logger = logging.getLogger("RealTimeCommerce.StochasticLogisticsTracking.DataQualityGate")


class LogisticsDataQualityGate:

    def __init__(self, settings: GlobalSettings) -> None:
        self.settings = settings
        self.max_allowed_delay_minutes = 1440.0

    def validate_inputs(self, df: DataFrame) -> None:
        logger.info("Инициализация распределенного аудита качества графовых метрик...")

        if df.isEmpty():
            logger.critical("[DQ_GATE] На вход подан пустой DataFrame вычислений Neo4j GDS!")
            raise DataQualityViolationException("Inbound graph dataset is completely empty")

        null_anomalies = df.select(
            F.count(F.when(F.col("order_id").isNull() | (F.trim(F.col("order_id")) == ""), 1)).alias("empty_orders"),
        ).first()

        if null_anomalies["empty_orders"] > 0:
            logger.error("[DQ_GATE] Обнаружено %d записей с пустым order_id!", null_anomalies["empty_orders"])
            raise DataQualityViolationException("Critical anomaly: Entity order_id cannot be null or empty string")

        nan_anomalies = df.select(
            F.count(F.when(F.isnan(F.col("calculated_delay_minutes")), 1)).alias("nan_delays"),
        ).first()

        if nan_anomalies["nan_delays"] > 0:
            logger.critical(
                "[DQ_GATE] Обнаружены NaN-выбросы в тензоре фичей! Delays NaN: %d",
                nan_anomalies["nan_delays"],
            )
            raise DataQualityViolationException("Mathematical tensor corruption: NaN values detected in GDS metrics")

        business_bounds_anomalies = df.select(
            F.count(F.when(F.col("calculated_delay_minutes") > self.max_allowed_delay_minutes, 1)).alias("extreme_delays"),
            F.count(F.when(F.col("calculated_delay_minutes") < 0.0, 1)).alias("negative_delays"),
            F.count(F.when(F.col("current_logistic_stage") < 1, 1)).alias("invalid_stages"),
        ).first()

        if business_bounds_anomalies["negative_delays"] > 0:
            logger.error("[DQ_GATE] Обнаружены отрицательные значения задержек поставок!")
            raise DataQualityViolationException("Physical law invariant breach: calculated_delay_minutes cannot be negative")

        if business_bounds_anomalies["invalid_stages"] > 0:
            logger.error("[DQ_GATE] Обнаружена некорректная стадия логистического конвейера (меньше 1)!")
            raise DataQualityViolationException("Process state invariant breach: current_logistic_stage out of bounds")

        if business_bounds_anomalies["extreme_delays"] > 0:
            logger.warning(
                "[DQ_GATE][LOGISTICS_ALERT] Обнаружено %d аномально высоких задержек (>24 часов).",
                business_bounds_anomalies["extreme_delays"],
            )

        logger.info("[УСПЕХ] Распределенный датасет успешно прошел все барьеры проверки качества данных")
