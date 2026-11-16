import os
import logging
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType

logger = logging.getLogger("RealTimeCommerce.StochasticLogisticsTracking.Transformer")


class StochasticLogisticsTransformer:

    def __init__(self, settings: dict = None) -> None:
        self.settings = settings or {}
        self.model_path = self.settings.get(
            "CATBOOST_LOGISTICS_MODEL_PATH", 
            "/opt/models/user_churn_logistics_v1.cbm"
        )

    def transform_and_predict(self, features_df: DataFrame) -> DataFrame:
        logger.info(f"Инициализация распределенного инференса. Путь к модели: {self.model_path}")
        
        current_model_path = self.model_path

        @F.pandas_udf(FloatType())
        def predict_churn_probability_udf(
            delay_minutes: pd.Series,
            logistic_stage: pd.Series,
            is_bottleneck: pd.Series
        ) -> pd.Series:
            from catboost import CatBoostClassifier
            
            if not hasattr(predict_churn_probability_udf, "model"):
                if not os.path.exists(current_model_path):
                    return pd.Series([0.5] * len(delay_minutes))
                
                predict_churn_probability_udf.model = CatBoostClassifier()
                predict_churn_probability_udf.model.load_model(current_model_path)
            
            input_features = pd.DataFrame({
                "calculated_delay_minutes": delay_minutes,
                "current_logistic_stage": logistic_stage,
                "is_bottleneck_flag": is_bottleneck
            })
            
            probabilities = predict_churn_probability_udf.model.predict_proba(input_features)[:, 1]
            return pd.Series(probabilities)

        output_df = features_df.withColumn(
            "churn_risk_score",
            predict_churn_probability_udf(
                F.col("calculated_delay_minutes"),
                F.col("current_logistic_stage"),
                F.col("is_bottleneck_flag")
            )
        )
        
        logger.info("Распределенный инференс CatBoost успешно применен к матрице признаков.")
        return output_df
