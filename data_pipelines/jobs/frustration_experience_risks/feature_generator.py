from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, FloatType, LongType

class FrustrationExperienceRisksFeatureGenerator:
    def __init__(self, settings: dict = None) -> None:
        self.settings = settings or {}

    def generate_features_matrix(
        self,
        raw_clicks_df: DataFrame,
        raw_perf_df: DataFrame,
        raw_search_df: DataFrame,
        neo4j_df: DataFrame,       
        es_raw_df: DataFrame,      
        target_date: str
    ) -> DataFrame:
        user_window = Window.partitionBy("user_id")
        user_order_window = Window.partitionBy("user_id").orderBy("timestamp")

        clicks_with_max = raw_clicks_df.withColumn("max_user_timestamp", F.max("timestamp").over(user_window))
        clicks_session_validated = clicks_with_max.filter(
            F.col("max_user_timestamp") < (F.current_timestamp() - F.expr("INTERVAL 30 MINUTES"))
        )

        clicks_lagged = clicks_session_validated \
            .withColumn("prev_timestamp", F.lag("timestamp", 1).over(user_order_window)) \
            .withColumn(
                "time_delta", 
                F.when(F.col("prev_timestamp").isNull(), F.lit(0))
                 .otherwise(F.col("timestamp").cast(LongType()) - F.col("prev_timestamp").cast(LongType()))
            ) \
            .withColumn(
                "is_new_session", 
                F.when((F.col("time_delta") > 1800) | F.col("prev_timestamp").isNull(), 1).otherwise(0)
            ) \
            .withColumn("prev_time_delta", F.coalesce(F.lag("time_delta", 1).over(user_order_window), F.lit(0))) \
            .withColumn(
                "velocity_acceleration", 
                F.when(F.col("prev_time_delta") == 0, 0).otherwise(F.col("time_delta") - F.col("prev_time_delta"))
            )


        clicks_indexed = clicks_lagged.withColumn("session_index", F.sum("is_new_session").over(user_order_window))

        pogo_sticking_expr = (
            "aggregate("
            "  zip_with(pages_path, transform(sequence(1, size(pages_path)), x -> x), (x, i) -> struct(x, i)), "
            "  0, "
            "  (acc, x) -> acc + case when x.i >= 3 and x.x = 'catalog' and pages_path[x.i-1] = 'product_card' and pages_path[x.i-2] = 'catalog' then 1 else 0 end"
            ")"
        )

        session_metrics = clicks_indexed.groupBy("user_id", "session_index").agg(
            F.count("*").alias("session_depth"),
            F.sum(F.when((F.col("time_delta") > 0) & (F.col("time_delta") < 0.25), 1).otherwise(0)).alias("rage_clicks_count"),
            F.slice(F.collect_list("page_type"), 1, 200).alias("pages_path"), 
            F.percentile_approx("time_delta", 0.90).alias("p90_frustration_time"),
            F.coalesce(F.stddev_pop("velocity_acceleration"), F.lit(0.0)).alias("click_velocity_volatility"),
            F.count_distinct("product_id").alias("unique_products_count"),
            F.count_distinct("category_id").alias("unique_categories_count"),
            F.last("warehouse_id").alias("last_warehouse_id"),
            F.last("pickup_point_id").alias("last_pickup_point_id"),
            F.last("product_id").alias("last_product_id")
        ).withColumn("session_pogo_sticking", F.expr(pogo_sticking_expr))

        final_click_metrics = session_metrics.groupBy("user_id").agg(
            F.max("session_depth").alias("max_session_depth"),
            F.max("p90_frustration_time").alias("p90_frustration"),
            F.sum("rage_clicks_count").alias("total_rage_clicks"),
            F.sum("session_pogo_sticking").alias("pogo_sticking_count"),
            F.max("click_velocity_volatility").alias("max_velocity_volatility"),
            F.max("unique_products_count").alias("max_unique_products"),
            F.max("unique_categories_count").alias("max_unique_categories"),
            F.last("last_warehouse_id").alias("origin_node_id"),
            F.last("last_pickup_point_id").alias("pickup_point_id"),
            F.last("last_product_id").alias("product_id")
        )


        perf_metrics = raw_perf_df.groupBy("user_id").agg(
            F.count("*").alias("total_perf_events"),
            F.sum(F.when(F.col("status_code") >= 500, 1).otherwise(0)).alias("api_errors_count"),
            F.percentile_approx("page_load_ms", 0.95).alias("p95_interface_render_ms"),
            F.avg("dns_resolution_ms").alias("avg_network_ms")
        )

        search_metrics = raw_search_df.groupBy("user_id").agg(
            F.count("*").alias("total_searches"),
            F.sum(F.when(F.col("action_type").isin('filter_reset', 'filter_change'), 1).otherwise(0)).alias("filter_manipulations"),
            F.max("scroll_depth_items").alias("max_search_scroll_depth")
        )

        es_features_df = es_raw_df.select(
            F.col("_source.product_id").alias("product_id"),
            F.element_at(F.col("fields.dynamic_promo_price"), 1).cast(FloatType()).alias("promo_price"),
            F.coalesce(F.col("_source.category_hierarchy.level"), F.lit(0)).cast(IntegerType()).alias("category_depth"),
            F.when(F.size(F.col("_source.bundle_compatible_ids")) > 0, 1).otherwise(0).cast(IntegerType()).alias("has_bundles")
        )

        ch_mart = final_click_metrics \
            .join(perf_metrics, "user_id", "left") \
            .join(search_metrics, "user_id", "left")

        ch_with_neo4j = ch_mart.join(
            neo4j_df,
            (ch_mart.origin_node_id == neo4j_df.origin_node_id) & 
            (ch_mart.pickup_point_id == neo4j_df.pickup_point_id),
            "left"
        ).drop(neo4j_df.origin_node_id).drop(neo4j_df.pickup_point_id)


        final_blend = ch_with_neo4j.join(es_features_df, "product_id", "left")

        b_p90_frustration = F.coalesce(F.col("p90_frustration"), F.lit(0.0))
        b_max_session_depth = F.coalesce(F.col("max_session_depth"), F.lit(1.0))
        b_route_time = F.coalesce(F.col("route_time"), F.lit(12.0))
        b_bottleneck_risk = F.coalesce(F.col("bottleneck_risk_factor"), F.lit(0.0))
        b_graph_hops = F.coalesce(F.col("graph_hops"), F.lit(0))
        b_promo_price = F.coalesce(F.col("promo_price"), F.lit(1.0))
        b_api_errors = F.coalesce(F.col("api_errors_count"), F.lit(0))

        final_blend = final_blend.withColumn(
            "frustration_index",
            F.log1p(b_p90_frustration) * 
            b_max_session_depth * 
            (b_route_time / 12.0) * 
            (F.lit(1.0) + b_bottleneck_risk) * 
            (b_graph_hops + F.lit(1))
        )

        final_blend = final_blend.withColumn(
            "financial_error_gravity",
            F.when(b_api_errors == 0, F.lit(0.0))
             .otherwise(b_promo_price * b_api_errors)
        )

        final_blend = final_blend.withColumn(
            "bundle_supply_vulnerability",
            F.coalesce(F.col("isolation_score"), F.lit(0.0)) * F.coalesce(F.col("has_bundles"), F.lit(0))
        )

        features_matrix = final_blend \
            .withColumn("log_date", F.to_date(F.lit(target_date))) \
            .withColumn("max_session_depth", F.coalesce(F.col("max_session_depth"), F.lit(0)).cast(IntegerType())) \
            .withColumn("p90_frustration", F.coalesce(F.col("p90_frustration"), F.lit(0.0)).cast(FloatType())) \
            .withColumn("total_rage_clicks", F.coalesce(F.col("total_rage_clicks"), F.lit(0)).cast(IntegerType())) \
            .withColumn("pogo_sticking_count", F.coalesce(F.col("pogo_sticking_count"), F.lit(0)).cast(IntegerType())) \
            .withColumn("max_velocity_volatility", F.coalesce(F.col("max_velocity_volatility"), F.lit(0.0)).cast(FloatType())) \
            .withColumn("max_unique_products", F.coalesce(F.col("max_unique_products"), F.lit(0)).cast(IntegerType())) \
            .withColumn("max_unique_categories", F.coalesce(F.col("max_unique_categories"), F.lit(0)).cast(IntegerType())) \
            .withColumn("total_perf_events", F.coalesce(F.col("total_perf_events"), F.lit(0)).cast(IntegerType())) \
            .withColumn("api_errors_count", F.coalesce(F.col("api_errors_count"), F.lit(0)).cast(IntegerType())) \
            .withColumn("p95_interface_render_ms", F.coalesce(F.col("p95_interface_render_ms"), F.lit(0.0)).cast(FloatType())) \
            .withColumn("avg_network_ms", F.coalesce(F.col("avg_network_ms"), F.lit(0.0)).cast(FloatType())) \
            .withColumn("total_searches", F.coalesce(F.col("total_searches"), F.lit(0)).cast(IntegerType())) \
            .withColumn("total_filter_resets", F.coalesce(F.col("filter_manipulations"), F.lit(0)).cast(IntegerType())) \
            .withColumn("max_search_scroll", F.coalesce(F.col("max_search_scroll_depth"), F.lit(0)).cast(IntegerType())) \
            .withColumn("route_time", F.coalesce(F.col("route_time"), F.lit(0.0)).cast(FloatType())) \
            .withColumn("route_cost", F.coalesce(F.col("route_cost"), F.lit(0.0)).cast(FloatType())) \
            .withColumn("route_jitter", F.coalesce(F.col("route_jitter"), F.lit(0.0)).cast(FloatType())) \
            .withColumn("bottleneck_risk_factor", F.coalesce(F.col("bottleneck_risk_factor"), F.lit(0.0)).cast(FloatType())) \
            .withColumn("graph_hops", F.coalesce(F.col("graph_hops"), F.lit(0)).cast(IntegerType())) \
            .withColumn("isolation_score", F.coalesce(F.col("isolation_score"), F.lit(0.0)).cast(FloatType())) \
            .withColumn("promo_price", F.coalesce(F.col("promo_price"), F.lit(0.0)).cast(FloatType())) \
            .withColumn("category_depth", F.coalesce(F.col("category_depth"), F.lit(0)).cast(IntegerType())) \
            .withColumn("has_bundles", F.coalesce(F.col("has_bundles"), F.lit(0)).cast(IntegerType())) \
            .withColumn("frustration_index", F.coalesce(F.col("frustration_index"), F.lit(0.0)).cast(FloatType())) \
            .withColumn("financial_error_gravity", F.coalesce(F.col("financial_error_gravity"), F.lit(0.0)).cast(FloatType())) \
            .withColumn("bundle_supply_vulnerability", F.coalesce(F.col("bundle_supply_vulnerability"), F.lit(0.0)).cast(FloatType())) \
            .withColumn("version", (F.current_timestamp().cast(LongType()) * 1000).cast(LongType())) 
        
        return features_matrix