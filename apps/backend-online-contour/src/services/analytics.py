from __future__ import annotations
import os
import math
import time
import asyncio
import logging
from typing import List, Any, Tuple
from catboost import CatBoostClassifier

from ..config import settings
from ..core.metrics import ML_INFERENCE_DURATION_SECONDS, ML_SHADOW_SCORE_DIFF
from ..models.cache_entities import UserOnlineFeaturesModel
from ..models.graph_entities import GraphOrderTrackingStatusModel

logger = logging.getLogger("rtc_ml_analytics")


class ChurnModelServingService:
    def __init__(self) -> None:
        self.model: CatBoostClassifier | None = None
        self.model_loaded: bool = False

        # Shadow Mode (Challenger model)
        self.shadow_model: CatBoostClassifier | None = None
        self.shadow_loaded: bool = False

    def load_model(self) -> None:
        # 1. Загрузка основной модели (Champion)
        if os.path.exists(settings.CATBOOST_MODEL_PATH):
            clf = CatBoostClassifier()
            clf.load_model(settings.CATBOOST_MODEL_PATH)
            self.model = clf
            self.model_loaded = True
            logger.info(f"[ML_SERVING] Champion CatBoost loaded: {settings.CATBOOST_MODEL_PATH}")
        else:
            self.model = None
            self.model_loaded = False
            logger.warning(
                f"[ML_SERVING] Champion model not found at {settings.CATBOOST_MODEL_PATH}. "
                "Using deterministic heuristic fallback."
            )

        # 2. Загрузка теневой модели (Challenger) для Shadow Mode
        if os.path.exists(settings.CATBOOST_SHADOW_MODEL_PATH):
            shadow_clf = CatBoostClassifier()
            shadow_clf.load_model(settings.CATBOOST_SHADOW_MODEL_PATH)
            self.shadow_model = shadow_clf
            self.shadow_loaded = True
            logger.info(f"[ML_SERVING][SHADOW] Challenger CatBoost loaded: {settings.CATBOOST_SHADOW_MODEL_PATH}")
        else:
            self.shadow_model = None
            self.shadow_loaded = False

    def warm_up(self, iterations: int = 5) -> None:
        dummy_user = UserOnlineFeaturesModel(
            user_id="WARMUP_USER",
            origin_node_id="WH_01",
            pickup_point_id="PP_001",
            promo_price=1000.0,
            has_bundles=1,
            route_time=15.0,
            bottleneck_risk_factor=0.3,
            graph_hops=2,
            isolation_score=0.2,
        )
        dummy_tracking = GraphOrderTrackingStatusModel(
            order_id="WARMUP_ORDER",
            product_id="PRD_0000001",
            calculated_delay_minutes=10.0,
            current_logistic_stage=3,
            is_bottleneck_flag=0,
            failing_node_id="NONE",
            bottleneck_risk_factor=0.3,
        )
        sample_vec = self.build_feature_vector_34(dummy_user, dummy_tracking)

        start = time.perf_counter()
        for _ in range(iterations):
            self._predict_sync(sample_vec)
            if self.shadow_loaded and self.shadow_model is not None:
                _ = self.shadow_model.predict_proba([sample_vec])
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        logger.info(
            f"[ML_SERVING][WARMUP] Completed {iterations} warm-up passes in {elapsed_ms:.2f}ms "
            f"(champion={self.model_loaded}, shadow={self.shadow_loaded})."
        )

    def build_feature_vector_34(
        self,
        user_features: UserOnlineFeaturesModel,
        live_tracking: GraphOrderTrackingStatusModel,
    ) -> List[Any]:
        origin_node = user_features.origin_node_id or live_tracking.origin_node_id or "WH_01"
        pickup_point = user_features.pickup_point_id or live_tracking.pickup_point_id or "PP_001"
        route_time = user_features.route_time
        bottleneck_risk = max(user_features.bottleneck_risk_factor, live_tracking.bottleneck_risk_factor)
        graph_hops = user_features.graph_hops

        frustration_index = float(
            math.log1p(max(0.0, user_features.p90_frustration))
            * user_features.max_session_depth
            * (route_time / 12.0)
            * (1.0 + bottleneck_risk)
            * (graph_hops + 1)
        )
        financial_error_gravity = (
            0.0
            if user_features.api_errors_count == 0
            else float(user_features.promo_price * user_features.api_errors_count)
        )
        bundle_supply_vulnerability = float(user_features.isolation_score * user_features.has_bundles)

        return [
            int(user_features.max_session_depth),
            float(user_features.p90_frustration),
            int(user_features.total_rage_clicks),
            int(user_features.pogo_sticking_count),
            float(user_features.max_velocity_volatility),
            int(user_features.max_unique_products),
            int(user_features.max_unique_categories),
            int(user_features.total_perf_events),
            int(user_features.api_errors_count),
            float(user_features.p95_interface_render_ms),
            float(user_features.avg_network_ms),
            int(user_features.total_searches),
            int(user_features.total_filter_resets),
            int(user_features.max_search_scroll),
            str(origin_node),
            str(pickup_point),
            float(user_features.promo_price),
            int(user_features.has_bundles),
            int(user_features.category_depth),
            str(live_tracking.product_id or "PRD_0000001"),        
            str(live_tracking.order_id or "ORD_0000001"),          
            float(live_tracking.calculated_delay_minutes),
            int(live_tracking.current_logistic_stage),
            int(live_tracking.is_bottleneck_flag),
            str(live_tracking.failing_node_id or "NONE"),         
            float(route_time),
            float(user_features.route_cost),
            float(user_features.route_jitter),
            float(bottleneck_risk),
            int(graph_hops),
            float(user_features.isolation_score),
            
            frustration_index,
            financial_error_gravity,
            bundle_supply_vulnerability,
        ]

    def _predict_sync(self, vector: List[Any]) -> float:
        if self.model is not None and self.model_loaded:
            probas = self.model.predict_proba([vector])
            return float(probas[0][1])

        delay_score = min(1.0, float(vector[21]) / 60.0) * 0.45
        bottleneck_score = float(vector[23]) * 0.25
        frustration_score = min(1.0, float(vector[1]) / 100.0) * 0.20
        api_err_score = min(1.0, float(vector[8]) / 5.0) * 0.10
        return round(min(0.99, delay_score + bottleneck_score + frustration_score + api_err_score), 4)

    def _run_shadow_inference_sync(self, vector: List[Any], champion_prob: float, order_id: str) -> None:
        if not self.shadow_loaded or self.shadow_model is None:
            return
        try:
            shadow_probas = self.shadow_model.predict_proba([vector])
            challenger_prob = float(shadow_probas[0][1])
            diff = abs(champion_prob - challenger_prob)
            ML_SHADOW_SCORE_DIFF.labels(
                champion_model="user_churn_retail_v2",
                challenger_model="user_churn_retail_challenger",
            ).observe(diff)
            if diff >= 0.15:
                logger.info(
                    f"[ML_SHADOW][DRIFT_ALERT] order_id={order_id} | "
                    f"champion={champion_prob:.4f} vs challenger={challenger_prob:.4f} (diff={diff:.4f})"
                )
        except Exception as err:
            logger.warning(f"[ML_SHADOW][FAIL] order_id={order_id}: {err}")

    async def predict_churn_probability(
        self,
        user_features: UserOnlineFeaturesModel,
        live_tracking: GraphOrderTrackingStatusModel,
        degraded_mode: bool = False,
    ) -> Tuple[float, float]:
        start_ts = time.perf_counter()
        vector = self.build_feature_vector_34(user_features, live_tracking)
        prob = await asyncio.to_thread(self._predict_sync, vector)
        latency_sec = time.perf_counter() - start_ts

        ML_INFERENCE_DURATION_SECONDS.labels(
            model_name="user_churn_retail_v2",
            degraded_mode=str(degraded_mode).lower(),
        ).observe(latency_sec)

        if self.shadow_loaded:
            from ..main import db_state

            if not db_state.reject_new_requests:
                db_state.spawn_background_task(
                    asyncio.to_thread(self._run_shadow_inference_sync, vector, prob, live_tracking.order_id),
                    name=f"shadow-ml-{live_tracking.order_id}",
                )

        return round(prob, 4), round(latency_sec * 1000.0, 2)


ml_serving_service = ChurnModelServingService()
