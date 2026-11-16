import pytest
from decimal import Decimal

from src.core.circuit_breaker import HighAvailabilityCircuitBreaker
from src.models.cache_entities import UserOnlineFeaturesModel
from src.models.graph_entities import GraphOrderTrackingStatusModel, GraphNodeHealthModel
from src.models.marts import NodeDynamicCapacityModel, DwhMarketMarginModel
from src.services.analytics import ChurnModelServingService
from src.services.topology_health import TopologyHealthService
from src.services.action_engine import ActionEngineService


def test_catboost_feature_vector_shape_and_types():
    serving = ChurnModelServingService()
    user_features = UserOnlineFeaturesModel(
        user_id="USR_001",
        max_session_depth=10,
        p90_frustration=50.0,
        api_errors_count=2,
        promo_price=1500.0,
        has_bundles=1,
        origin_node_id="WH_02",
        pickup_point_id="PP_010",
        route_time=18.0,
        bottleneck_risk_factor=0.6,
        graph_hops=3,
        isolation_score=0.5,
    )
    tracking = GraphOrderTrackingStatusModel(
        order_id="ORD_001",
        product_id="PRD_999",
        calculated_delay_minutes=35.0,
        current_logistic_stage=5,
        is_bottleneck_flag=1,
        failing_node_id="HUB_003",
        bottleneck_risk_factor=0.8,
    )

    vec = serving.build_feature_vector_34(user_features, tracking)

    assert len(vec) == 34
    # Проверяем 5 категориальных признаков на индексах [14, 15, 19, 20, 24]
    for cat_idx in (14, 15, 19, 20, 24):
        assert isinstance(vec[cat_idx], str)

    # Проверяем вычисленные синтетические признаки [31, 32, 33]
    assert vec[31] > 0.0  # frustration_index
    assert vec[32] == 3000.0  # financial_error_gravity (1500 * 2)
    assert vec[33] == 0.5  # bundle_supply_vulnerability (0.5 * 1)


def test_topology_health_passport_zscore_calculation():
    breaker = HighAvailabilityCircuitBreaker(name="test-cb")
    svc = TopologyHealthService(
        postgres_repo=None,  # type: ignore
        redis_repo=None,  # type: ignore
        neo4j_repo=None,  # type: ignore
        postgres_breaker=breaker,
        redis_breaker=breaker,
        neo4j_breaker=breaker,
    )

    cap = NodeDynamicCapacityModel(
        node_id="HUB_001",
        node_type="HUB",
        base_capacity_packages=1000,
        current_capacity_packages=200,  # 80% деградация
        bottleneck_risk_factor=0.9,
        is_active_block=1,
    )
    graph = GraphNodeHealthModel(
        node_id="HUB_001",
        status="CONGESTED",
        is_bottleneck_flag=1,
        bottleneck_risk_factor=0.9,
        affected_personnel_count=25,
    )

    passport = svc._compute_passport_metrics(cap, graph)
    assert passport.capacity_utilization_pct == 80.0
    assert passport.performance_lag_zscore >= 3.0
    assert passport.health_grade == "CRITICAL_BLOCKED"
    assert passport.affected_personnel_count == 25


def test_circuit_breaker_state_transitions():
    cb = HighAvailabilityCircuitBreaker(name="unit-cb", failure_threshold=2, recovery_timeout=60.0)
    assert cb.allow_request() is True
    assert cb.state == "CLOSED"

    cb.record_failure(RuntimeError("timeout 1"))
    assert cb.state == "CLOSED"

    cb.record_failure(RuntimeError("timeout 2"))
    assert cb.state == "OPEN"
    assert cb.allow_request() is False

    cb.record_success()
    assert cb.state == "CLOSED"
    assert cb.allow_request() is True


def test_action_engine_unit_economics_cap():
    breaker = HighAvailabilityCircuitBreaker(name="test-ae")
    svc = ActionEngineService(
        postgres_repo=None,  # type: ignore
        redis_repo=None,  # type: ignore
        neo4j_repo=None,  # type: ignore
        ml_service=None,  # type: ignore
        redis_breaker=breaker,
        neo4j_breaker=breaker,
        postgres_breaker=breaker,
    )

    # Обычный сегмент с низкой маржой заказа (150 руб -> 80% = 120 руб)
    margin = DwhMarketMarginModel(order_id="ORD_10", order_margin_rub=Decimal("150.00"), is_express_grocery=0)
    amount = svc._calculate_compensation_rub(
        churn_prob=0.82,
        user_ltv=5000.0,
        customer_segment="REGULAR",
        margin_info=margin,
        delay_minutes=40.0,
    )
    assert amount == Decimal("120.00")
