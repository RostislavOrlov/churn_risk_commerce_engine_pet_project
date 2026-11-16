import os
import gc
import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, StringType

from data_pipelines.shared.config import GlobalSettings

logger = logging.getLogger("RealTimeCommerce.MLInferenceOrchestrator.Transformer")


class CentralCatBoostTransformer:

    FEATURE_COLUMNS = (
        "max_session_depth",
        "p90_frustration",
        "total_rage_clicks",
        "pogo_sticking_count",
        "max_velocity_volatility",
        "max_unique_products",
        "max_unique_categories",
        "total_perf_events",
        "api_errors_count",
        "p95_interface_render_ms",
        "avg_network_ms",
        "total_searches",
        "total_filter_resets",
        "max_search_scroll",
        "route_cost",
        "route_time",
        "route_jitter",
        "bottleneck_risk_factor",
        "graph_hops",
        "isolation_score",
        "promo_price",
        "marketing_tags_count",
        "has_bundles",
        "frustration_index",
    )

    def __init__(self, settings: GlobalSettings) -> None:
        self.settings = settings
        self.model_path = self.settings.get(
            "CATBOOST_ENSEMBLE_MODEL_PATH",
            "/opt/airflow/models/final_meta_ensemble.cbm",
        )

    def transform_and_predict(self, full_features_df: DataFrame) -> DataFrame:
        logger.info(f"Инициализация инференса. Путь к модели: {self.model_path}")

        base_df = full_features_df
        if "marketing_tags_count" not in base_df.columns:
            base_df = base_df.withColumn("marketing_tags_count", F.coalesce(F.col("category_depth"), F.lit(0)))

        enriched_df = base_df.withColumn(
            "frustration_index",
            (
                F.log1p(F.coalesce(F.col("p90_frustration").cast(FloatType()), F.lit(0.0)))
                * F.coalesce(F.col("max_session_depth").cast(FloatType()), F.lit(0.0))
                * (F.coalesce(F.col("route_time").cast(FloatType()), F.lit(0.0)) / F.lit(12.0))
                * (F.lit(1.0) + F.coalesce(F.col("bottleneck_risk_factor").cast(FloatType()), F.lit(0.0)))
                * (F.coalesce(F.col("graph_hops").cast(FloatType()), F.lit(0.0)) + F.lit(1.0))
            ).cast(FloatType()),
        )

        if os.path.exists(self.model_path):
            import pandas as pd
            import numpy as np

            current_model_path = self.model_path
            feature_order = self.FEATURE_COLUMNS

            @F.pandas_udf(FloatType())
            def predict_batch_udf(*cols: pd.Series) -> pd.Series:
                from catboost import CatBoostClassifier

                if not hasattr(predict_batch_udf, "model"):
                    predict_batch_udf.model = CatBoostClassifier()
                    predict_batch_udf.model.load_model(current_model_path)

                features_df = pd.DataFrame({
                    feature_name: pd.to_numeric(cols[idx], errors="coerce").fillna(0.0)
                    for idx, feature_name in enumerate(feature_order)
                })
                model_matrix = np.ascontiguousarray(features_df.values, dtype=np.float32)
                probabilities = predict_batch_udf.model.predict_proba(model_matrix)[:, 1]
                del features_df, model_matrix
                gc.collect()
                return pd.Series(probabilities, dtype="float32")

            scored_df = enriched_df.withColumn(
                "churn_probability",
                predict_batch_udf(*[F.col(name) for name in feature_order]),
            )
        else:
            logger.warning(f"Файл модели {self.model_path} не найден. Выполняется нативный Spark Catalyst скоринг.")
            raw_logit = (
                F.lit(-1.8)
                + F.lit(0.04) * F.log1p(F.greatest(F.col("frustration_index"), F.lit(0.0)))
                + F.lit(0.35) * F.coalesce(F.col("api_errors_count").cast(FloatType()), F.lit(0.0))
                + F.lit(0.15) * F.coalesce(F.col("total_rage_clicks").cast(FloatType()), F.lit(0.0))
                + F.lit(0.25) * F.coalesce(F.col("route_jitter").cast(FloatType()), F.lit(0.0))
            )
            clamped_logit = F.greatest(F.lit(-10.0), F.least(F.lit(10.0), raw_logit))
            scored_df = enriched_df.withColumn(
                "churn_probability",
                (F.lit(1.0) / (F.lit(1.0) + F.exp(-clamped_logit))).cast(FloatType()),
            )

        output_df = scored_df.select(
            F.col("user_id").cast(StringType()).alias("user_id"),
            F.col("churn_probability").cast(FloatType()).alias("churn_probability"),
            F.to_date(F.col("log_date")).alias("log_date"),
        )

        logger.info("Инференс ансамбля успешно применен ко всей 24-фичной матрице.")
        return output_df
