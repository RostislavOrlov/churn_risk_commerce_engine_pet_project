import os
import numpy as np
from catboost import CatBoostClassifier


def create_and_save_mock_models():
    current_file_path = os.path.abspath(__file__)
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(current_file_path))))

    model_dir = os.path.join(project_root, ".models")
    champion_path = os.path.join(model_dir, "user_churn_retail_v2.cbm")
    challenger_path = os.path.join(model_dir, "user_churn_retail_challenger.cbm")

    os.makedirs(model_dir, exist_ok=True)

    FEATURE_COLUMNS = [
        "max_session_depth", "p90_frustration", "total_rage_clicks", "pogo_sticking_count",
        "max_velocity_volatility", "max_unique_products", "max_unique_categories", "total_perf_events",
        "api_errors_count", "p95_interface_render_ms", "avg_network_ms", "total_searches",
        "total_filter_resets", "max_search_scroll", "warehouse_id", "pickup_point_id",
        "promo_price", "has_bundles", "category_depth",
        "product_id", "order_id", "calculated_delay_minutes", "current_logistic_stage",
        "is_bottleneck_flag", "failing_node_id", "route_time", "route_cost", "route_jitter",
        "bottleneck_risk_factor", "graph_hops", "isolation_score",
        "frustration_index", "financial_error_gravity", "bundle_supply_vulnerability"
    ]

    total_features_count = len(FEATURE_COLUMNS)
    cat_features_indices = [14, 15, 19, 20, 24]

    rng = np.random.default_rng(seed=42)
    dummy_samples_count = 150

    X_dummy = rng.random((dummy_samples_count, total_features_count)).astype(object)

    for row in range(dummy_samples_count):
        X_dummy[row, 14] = f"WH_{rng.integers(1, 10):02d}"
        X_dummy[row, 15] = f"PP_{rng.integers(1, 50):03d}"
        X_dummy[row, 19] = f"PRD_{rng.integers(1, 1000):07d}"
        X_dummy[row, 20] = f"ORD_{rng.integers(10000, 99999):07d}"
        X_dummy[row, 24] = f"HUB_{rng.integers(1, 20):03d}"

    y_dummy = rng.integers(0, 2, size=(dummy_samples_count,))

    champion_model = CatBoostClassifier(
        iterations=10,
        depth=4,
        learning_rate=0.05,
        loss_function="Logloss",
        random_seed=42,
    )
    champion_model.fit(
        X_dummy,
        y_dummy,
        cat_features=cat_features_indices,
        verbose=False,
    )
    champion_model.save_model(champion_path)

    challenger_model = CatBoostClassifier(
        iterations=25,
        depth=6,
        learning_rate=0.08,
        loss_function="Logloss",
        random_seed=99,
    )
    challenger_model.fit(
        X_dummy,
        y_dummy,
        cat_features=cat_features_indices,
        verbose=False,
    )
    challenger_model.save_model(challenger_path)

    print(f"CATBOOST_MODEL_PATH={champion_path}")
    print(f"CATBOOST_SHADOW_MODEL_PATH={challenger_path}")


if __name__ == "__main__":
    create_and_save_mock_models()
