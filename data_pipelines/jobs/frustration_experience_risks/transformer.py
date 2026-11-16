import os
import gc
import logging
import numpy as np
import pandas as pd
from typing import Dict, Any, Optional
from catboost import CatBoostClassifier
import time

from data_pipelines.shared.config import GlobalSettings

logger = logging.getLogger("RealTimeCommerce.FrustrationExperienceRisks.Transformer")


class FrustrationExperienceRisksTransformer:
    FEATURE_COLUMNS = (
        'max_session_depth', 'p90_frustration', 'total_rage_clicks', 'pogo_sticking_count',
        'max_velocity_volatility', 'max_unique_products', 'max_unique_categories', 'total_perf_events',
        'api_errors_count', 'p95_interface_render_ms', 'avg_network_ms', 'total_searches',
        'total_filter_resets', 'max_search_scroll', 'route_cost', 'route_time', 'route_jitter',
        'bottleneck_risk', 'graph_hops', 'isolation_score', 'promo_price', 'marketing_tags_count',
        'has_bundles', 'frustration_index'
    )

    def __init__(self, settings: GlobalSettings) -> None:
        self._settings = settings
        self._model: Optional[CatBoostClassifier] = self._load_binary_model()

    def _load_binary_model(self) -> CatBoostClassifier:
        model_path = os.getenv("CATBOOST_MODEL_PATH", "/opt/models/user_churn_retail_v2.cbm")
        if not os.path.exists(model_path):
            logger.critical(f"Критический сбой: Бинарный файл CatBoost отсутствует по пути {model_path}")
            raise FileNotFoundError(f"CatBoost production model file missing strictly at: {model_path}")
        
        model = CatBoostClassifier()
        model.load_model(model_path)
        logger.info(f"CatBoost модель успешно загружена в RAM из локального хранилища: {model_path}")
        return model

    def transform_and_predict(
        self, 
        df: pd.DataFrame, 
        logistics_map: Dict[str, Dict[str, Any]], 
        marketing_map: Dict[str, Dict[str, Any]]
    ) -> pd.DataFrame:
        if df.empty:
            logger.warning("Получен пустой DataFrame на этапе трансформации. Прерывание итерации конвейера.")
            df['churn_probability'] = np.nan
            return df

        start_time = time.perf_counter() if 'time' in globals() else 0.0
        logger.info(f"Старт CPU-bound ML-трансформации батча. Размерность: {df.shape[0]} строк.")

        fallback_cfg = self._settings.get("logistics_marketing_fallbacks", {
            "route_cost": 50.0, "route_time": 24.0, "route_jitter": 0.0,
            "bottleneck_risk": 0.0, "graph_hops": 2, "isolation_score": 0.0,
            "promo_price": 0.0, "marketing_tags_count": 0, "has_bundles": 0
        })

        cost_flat = {k: v.get("route_cost", fallback_cfg["route_cost"]) for k, v in logistics_map.items()}
        time_flat = {k: v.get("route_time", fallback_cfg["route_time"]) for k, v in logistics_map.items()}
        jitter_flat = {k: v.get("route_jitter", fallback_cfg["route_jitter"]) for k, v in logistics_map.items()}
        bottleneck_flat = {k: v.get("bottleneck_risk", fallback_cfg["bottleneck_risk"]) for k, v in logistics_map.items()}
        hops_flat = {k: v.get("graph_hops", fallback_cfg["graph_hops"]) for k, v in logistics_map.items()}
        isolation_flat = {k: v.get("isolation_score", fallback_cfg["isolation_score"]) for k, v in logistics_map.items()}

        price_flat = {k: v.get("promo_price", fallback_cfg["promo_price"]) for k, v in marketing_map.items()}
        tags_flat = {k: v.get("marketing_tags_count", fallback_cfg["marketing_tags_count"]) for k, v in marketing_map.items()}
        bundles_flat = {k: v.get("has_bundles", fallback_cfg["has_bundles"]) for k, v in marketing_map.items()}

        df['route_cost'] = df['geo_key'].map(cost_flat).fillna(fallback_cfg["route_cost"]).astype(np.float32)
        df['route_time'] = df['geo_key'].map(time_flat).fillna(fallback_cfg["route_time"]).astype(np.float32)
        df['route_jitter'] = df['geo_key'].map(jitter_flat).fillna(fallback_cfg["route_jitter"]).astype(np.float32)
        df['bottleneck_risk'] = df['geo_key'].map(bottleneck_flat).fillna(fallback_cfg["bottleneck_risk"]).astype(np.float32)
        df['graph_hops'] = df['geo_key'].map(hops_flat).fillna(fallback_cfg["graph_hops"]).astype(np.int16)
        df['isolation_score'] = df['geo_key'].map(isolation_flat).fillna(fallback_cfg["isolation_score"]).astype(np.float32)

        df['promo_price'] = df['product_id'].map(price_flat).fillna(fallback_cfg["promo_price"]).astype(np.float32)
        df['marketing_tags_count'] = df['product_id'].map(tags_flat).fillna(fallback_cfg["marketing_tags_count"]).astype(np.int16)
        df['has_bundles'] = df['product_id'].map(bundles_flat).fillna(fallback_cfg["has_bundles"]).astype(np.int16)

        df.drop(columns=['geo_key'], inplace=True)

        base_features_to_cast = [
            'max_session_depth', 'p90_frustration', 'total_rage_clicks', 'pogo_sticking_count',
            'max_velocity_volatility', 'max_unique_products', 'max_unique_categories', 'total_perf_events',
            'api_errors_count', 'p95_interface_render_ms', 'avg_network_ms', 'total_searches',
            'total_filter_resets', 'max_search_scroll'
        ]
        for col in base_features_to_cast:
            df[col] = df[col].fillna(0.0).astype(np.float32)

        p90_frustration_arr = df['p90_frustration'].to_numpy(dtype=np.float32)
        max_session_depth_arr = df['max_session_depth'].to_numpy(dtype=np.float32)
        route_time_arr = df['route_time'].to_numpy(dtype=np.float32)
        bottleneck_risk_arr = df['bottleneck_risk'].to_numpy(dtype=np.float32)
        graph_hops_arr = df['graph_hops'].to_numpy(dtype=np.float32)

        df['frustration_index'] = (
            np.log1p(p90_frustration_arr) * 
            max_session_depth_arr * 
            (route_time_arr / 12.0) * 
            (1.0 + bottleneck_risk_arr) * 
            (graph_hops_arr + 1.0)
        ).astype(np.float32)

        model_features_matrix = np.ascontiguousarray(
            df[list(self.FEATURE_COLUMNS)].values, 
            dtype=np.float32
        )

        if self._model is not None:
            df['churn_probability'] = self._model.predict_proba(model_features_matrix)[:, 1]
        else:
            raise RuntimeError("Попытка инференса на неинициализированной модели CatBoost.")

        if start_time > 0:
            duration = time.perf_counter() - start_time
            logger.info(f"Инференс батча успешно завершен за {duration:.4f} секунд.")

        del model_features_matrix, p90_frustration_arr, max_session_depth_arr, route_time_arr, bottleneck_risk_arr, graph_hops_arr
        del cost_flat, time_flat, jitter_flat, bottleneck_flat, hops_flat, isolation_flat, price_flat, tags_flat, bundles_flat
        gc.collect()

        return df

    def close(self) -> None:
        if self._model is not None:
            logger.info("Выгрузка бинарного контекста модели CatBoost Classifier из оперативной памяти...")
            del self._model
            self._model = None
            gc.collect()

    def __del__(self) -> None:
        """Автоматический триггер очистки при деструктуризации объекта сборщиком мусора."""
        self.close()
