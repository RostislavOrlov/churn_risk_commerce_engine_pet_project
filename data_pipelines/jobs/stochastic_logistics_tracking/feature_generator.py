from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ShortType, FloatType, StringType


class StochasticLogisticsFeatureGenerator:

    def __init__(self, settings: dict = None) -> None:
        self.settings = settings or {}

    def generate_logistics_features(
        self,
        spark: SparkSession,
        neo4j_gds_df: DataFrame,
        target_date: str,
    ) -> DataFrame:
        final_matrix = neo4j_gds_df.select(
            F.to_date(F.lit(target_date)).alias("log_date"),
            F.col("order_id").cast(StringType()).alias("order_id"),
            F.coalesce(F.col("calculated_delay_minutes").cast(FloatType()), F.lit(0.0)).alias("calculated_delay_minutes"),
            F.coalesce(F.col("current_logistic_stage").cast(ShortType()), F.lit(1).cast(ShortType())).alias("current_logistic_stage"),
            F.coalesce(F.col("is_bottleneck_flag").cast(ShortType()), F.lit(0).cast(ShortType())).alias("is_bottleneck_flag"),
            F.coalesce(F.col("failing_node_id").cast(StringType()), F.lit("")).alias("failing_node_id"),
        ).dropDuplicates(["log_date", "order_id"])

        final_matrix_cleaned = final_matrix.na.fill(0.0).na.fill(0).na.fill("")

        return final_matrix_cleaned
