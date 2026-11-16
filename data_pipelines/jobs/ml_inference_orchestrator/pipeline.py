import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from data_pipelines.shared.config import GlobalSettings, settings
from data_pipelines.jobs.ml_inference_orchestrator.transformer import CentralCatBoostTransformer

logger = logging.getLogger("RealTimeCommerce.MLInferenceOrchestrator.Pipeline")


class CentralMLInferencePipeline:

    def __init__(self, settings_obj: GlobalSettings = settings) -> None:
        self.settings = settings_obj
        self.transformer = CentralCatBoostTransformer(self.settings)

        self.spark = (
            SparkSession.builder.appName("RealTimeCommerce.CentralMLInference.Engine")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate()
        )

    def _get_postgres_jdbc_config(self) -> tuple:
        pg_cfg = getattr(self.settings, "postgres_config", {})
        host = pg_cfg.get("host") or self.settings.get("POSTGRES_HOST", "postgres-master")
        port = pg_cfg.get("port") or self.settings.get("POSTGRES_PORT", 5432)
        database = pg_cfg.get("database") or self.settings.get("POSTGRES_DB", "retail_oltp")
        user = pg_cfg.get("user") or self.settings.get("POSTGRES_USER", "retail_user")
        password = pg_cfg.get("password") or self.settings.get("POSTGRES_PASSWORD", "retail_password")

        jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"
        properties = {
            "user": str(user),
            "password": str(password),
            "driver": "org.postgresql.Driver",
            "batchsize": "25000",
            "stringtype": "unspecified",
        }
        return jdbc_url, properties

    def execute_sync(self, target_date: str) -> None:
        logger.info(f"Запуск In-Memory конвейера слияния гетерогенных признаков за дату {target_date}...")
        jdbc_url, pg_properties = self._get_postgres_jdbc_config()

        try:
            logger.info("Извлечение интерфейсных признаков из stg_analytics.ux_features_buffer...")
            ux_query = f"SELECT * FROM stg_analytics.ux_features_buffer WHERE log_date = '{target_date}'"
            ux_df = (
                self.spark.read.format("jdbc")
                .option("url", jdbc_url)
                .option("query", ux_query)
                .options(**pg_properties)
                .load()
            )

            logger.info("Извлечение логистических признаков из stg_analytics.logistics_features_buffer...")
            logistics_query = f"SELECT * FROM stg_analytics.logistics_features_buffer WHERE log_date = '{target_date}'"
            logistics_df = (
                self.spark.read.format("jdbc")
                .option("url", jdbc_url)
                .option("query", logistics_query)
                .options(**pg_properties)
                .load()
            )

            logger.info("Запуск распределенного FULL OUTER JOIN в оперативной памяти Spark-кластера...")
            join_conditions = (ux_df.log_date == logistics_df.log_date) & (ux_df.user_id == logistics_df.user_id)

            full_features_df = ux_df.join(logistics_df, join_conditions, "full_outer").select(
                F.coalesce(ux_df.log_date, logistics_df.log_date).alias("log_date"),
                F.coalesce(ux_df.user_id, logistics_df.user_id).alias("user_id"),
                F.coalesce(ux_df.max_session_depth, F.lit(0)).alias("max_session_depth"),
                F.coalesce(ux_df.p90_frustration, F.lit(0.0)).alias("p90_frustration"),
                F.coalesce(ux_df.total_rage_clicks, F.lit(0)).alias("total_rage_clicks"),
                F.coalesce(ux_df.pogo_sticking_count, F.lit(0)).alias("pogo_sticking_count"),
                F.coalesce(ux_df.max_velocity_volatility, F.lit(0.0)).alias("max_velocity_volatility"),
                F.coalesce(ux_df.max_unique_products, F.lit(0)).alias("max_unique_products"),
                F.coalesce(ux_df.max_unique_categories, F.lit(0)).alias("max_unique_categories"),
                F.coalesce(ux_df.total_perf_events, F.lit(0)).alias("total_perf_events"),
                F.coalesce(ux_df.api_errors_count, F.lit(0)).alias("api_errors_count"),
                F.coalesce(ux_df.p95_interface_render_ms, F.lit(0.0)).alias("p95_interface_render_ms"),
                F.coalesce(ux_df.avg_network_ms, F.lit(0.0)).alias("avg_network_ms"),
                F.coalesce(ux_df.total_searches, F.lit(0)).alias("total_searches"),
                F.coalesce(ux_df.total_filter_resets, F.lit(0)).alias("total_filter_resets"),
                F.coalesce(ux_df.max_search_scroll, F.lit(0)).alias("max_search_scroll"),
                F.coalesce(ux_df.origin_node_id, logistics_df.origin_node_id, F.lit("")).alias("origin_node_id"),
                F.coalesce(ux_df.pickup_point_id, logistics_df.pickup_point_id, F.lit("")).alias("pickup_point_id"),
                F.coalesce(ux_df.promo_price, F.lit(0.0)).alias("promo_price"),
                F.coalesce(ux_df.has_bundles, F.lit(0)).alias("has_bundles"),
                F.coalesce(ux_df.category_depth, F.lit(0)).alias("category_depth"),
                F.coalesce(logistics_df.product_id, F.lit("")).alias("product_id"),
                F.coalesce(logistics_df.order_id, F.lit("")).alias("order_id"),
                F.coalesce(logistics_df.route_time.cast("float"), F.lit(0.0)).alias("route_time"),
                F.coalesce(logistics_df.route_cost.cast("float"), F.lit(0.0)).alias("route_cost"),
                F.coalesce(logistics_df.route_jitter.cast("float"), F.lit(0.0)).alias("route_jitter"),
                F.coalesce(logistics_df.calculated_delay_minutes.cast("float"), F.lit(0.0)).alias("calculated_delay_minutes"),
                F.coalesce(logistics_df.current_logistic_stage.cast("int"), F.lit(1)).alias("current_logistic_stage"),
                F.coalesce(logistics_df.is_bottleneck_flag.cast("int"), F.lit(0)).alias("is_bottleneck_flag"),
                F.coalesce(logistics_df.failing_node_id, F.lit("")).alias("failing_node_id"),
                F.coalesce(logistics_df.bottleneck_risk_factor.cast("float"), F.lit(0.0)).alias("bottleneck_risk_factor"),
                F.coalesce(logistics_df.graph_hops.cast("int"), F.lit(0)).alias("graph_hops"),
                F.coalesce(logistics_df.isolation_score.cast("float"), F.lit(0.0)).alias("isolation_score"),
                (
                    F.log1p(F.coalesce(ux_df.p90_frustration, F.lit(0.0)))
                    * F.coalesce(ux_df.max_session_depth, F.lit(0))
                    * (F.coalesce(logistics_df.route_time.cast("float"), F.lit(0.0)) / F.lit(12.0))
                    * (F.lit(1.0) + F.coalesce(logistics_df.bottleneck_risk_factor.cast("float"), F.lit(0.0)))
                    * (F.coalesce(logistics_df.graph_hops.cast("int"), F.lit(0)) + F.lit(1))
                )
                .cast("float")
                .alias("frustration_index"),
                F.when(F.coalesce(ux_df.api_errors_count, F.lit(0)) == 0, F.lit(0.0))
                .otherwise(F.coalesce(ux_df.promo_price, F.lit(0.0)) * F.coalesce(ux_df.api_errors_count, F.lit(0)))
                .cast("float")
                .alias("financial_error_gravity"),
                (
                    F.coalesce(logistics_df.isolation_score.cast("float"), F.lit(0.0))
                    * F.coalesce(ux_df.has_bundles, F.lit(0))
                )
                .cast("float")
                .alias("bundle_supply_vulnerability"),
            )

            cached_features_df = full_features_df.cache()
            total_rows = cached_features_df.count()
            logger.info(f"Сквозная матрица из {total_rows} профилей пользователей собрана в RAM.")

            final_predictions_df = self.transformer.transform_and_predict(cached_features_df)

            logger.info("Запись рассчитанных вероятностей оттока в stg_analytics.stg_ml_user_churn_scores...")
            final_predictions_df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", "stg_analytics.stg_ml_user_churn_scores") \
                .options(**pg_properties) \
                .mode("append") \
                .save()

            logger.info("Все предикты успешно закоммичены в DWH!")

        finally:
            self.spark.stop()
