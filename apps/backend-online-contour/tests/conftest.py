import uuid
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from time import monotonic
from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

from src.config import settings
from src.main import app, db_state
from src.api.dependencies import (
    get_master_session,
    get_replica_session,
    get_postgres_repository,
    get_redis_repository,
    get_neo4j_repository,
)
from src.models.cache_entities import UserOnlineFeaturesModel
from src.models.graph_entities import GraphOrderTrackingStatusModel, GraphNodeHealthModel
from src.models.marts import (
    DwhUserLtvModel,
    DwhMarketMarginModel,
    PreemptiveCompensationModel,
    IncidentLogModel,
    NodeDynamicCapacityModel,
)


@pytest.fixture(autouse=True)
def disable_external_auth(monkeypatch):
    monkeypatch.setattr(settings, "AUTH_ENABLED", False)
    db_state.is_ready = True
    db_state.is_shutting_down = False
    db_state.reject_new_requests = False
    db_state._readiness_cache = (monotonic() + 3600.0, True, {"redis": "UP", "postgres_replica": "UP"})


@pytest.fixture
def sample_incident_id() -> uuid.UUID:
    return uuid.UUID("11111111-2222-3333-4444-555555555555")


@pytest.fixture
def mock_pg_repo(sample_incident_id: uuid.UUID) -> MagicMock:
    repo = MagicMock()

    repo.fetch_user_ltv = AsyncMock(
        return_value=DwhUserLtvModel(
            user_id="USR_0000042",
            long_term_value_rub=Decimal("65000.00"),
            customer_segment="VIP_RETENTION",
        )
    )
    repo.fetch_market_margin = AsyncMock(
        return_value=DwhMarketMarginModel(
            order_id="ORD_0000042",
            order_margin_rub=Decimal("850.00"),
            is_express_grocery=1,
        )
    )

    async def _fake_insert_comp(session, user_id, order_id, churn_risk_score, voucher_code, discount_amount_rub):
        return PreemptiveCompensationModel(
            compensation_id=101,
            user_id=user_id,
            order_id=order_id,
            churn_risk_score=churn_risk_score,
            voucher_code=voucher_code,
            discount_amount_rub=discount_amount_rub,
            is_sent_flag=0,
            sent_at=None,
            expires_at=datetime.now(timezone.utc) + timedelta(days=7),
        )

    repo.insert_preemptive_compensation = AsyncMock(side_effect=_fake_insert_comp)
    repo.fetch_user_compensations = AsyncMock(
        return_value=[
            PreemptiveCompensationModel(
                compensation_id=101,
                user_id="USR_0000042",
                order_id="ORD_0000042",
                churn_risk_score=0.88,
                voucher_code="CARE-ABCD1234",
                discount_amount_rub=Decimal("400.00"),
                is_sent_flag=1,
                sent_at=datetime.now(timezone.utc),
                expires_at=datetime.now(timezone.utc) + timedelta(days=7),
            )
        ]
    )
    repo.fetch_active_incidents = AsyncMock(
        return_value=[
            IncidentLogModel(
                incident_id=sample_incident_id,
                order_id="ORD_0000042",
                failing_node_id="HUB_001",
                logistic_stage_idx=5,
                calculated_delay_minutes=47.5,
                incident_status="RAISED",
                assigned_team="LINEHAUL_OPS",
                created_at=datetime.now(timezone.utc),
            )
        ]
    )
    repo.update_incident_to_rerouted = AsyncMock(
        return_value=IncidentLogModel(
            incident_id=sample_incident_id,
            order_id="ORD_0000042",
            failing_node_id="HUB_001",
            logistic_stage_idx=5,
            calculated_delay_minutes=47.5,
            incident_status="REROUTED",
            assigned_team="DISPATCH_NIGHT_SHIFT",
        )
    )
    repo.fetch_blocked_or_risky_nodes = AsyncMock(
        return_value=[
            NodeDynamicCapacityModel(
                node_id="HUB_001",
                node_type="HUB",
                base_capacity_packages=5000,
                current_capacity_packages=1200,
                bottleneck_risk_factor=0.89,
                is_active_block=1,
            )
        ]
    )
    repo.fetch_node_capacity_by_id = AsyncMock(
        return_value=NodeDynamicCapacityModel(
            node_id="HUB_001",
            node_type="HUB",
            base_capacity_packages=5000,
            current_capacity_packages=1200,
            bottleneck_risk_factor=0.89,
            is_active_block=1,
        )
    )
    return repo


@pytest.fixture
def mock_redis_repo() -> MagicMock:
    repo = MagicMock()
    repo.get_user_online_features = AsyncMock(
        return_value=UserOnlineFeaturesModel(
            user_id="USR_0000042",
            max_session_depth=14,
            p90_frustration=82.5,
            total_rage_clicks=6,
            api_errors_count=3,
            promo_price=1990.0,
            has_bundles=1,
            category_depth=3,
            user_ltv=65000.0,
            customer_segment="VIP_RETENTION",
            route_time=24.0,
            bottleneck_risk_factor=0.85,
            graph_hops=4,
            isolation_score=0.7,
        )
    )
    # Имитируем кэш-мисс для active_incidents и blocked_nodes, чтобы проверить фолбэк на Postgres Replica
    repo.get_active_incidents_cached = AsyncMock(return_value=None)
    repo.set_active_incidents_cached = AsyncMock(return_value=None)
    repo.invalidate_active_incidents = AsyncMock(return_value=None)
    repo.get_blocked_nodes_cached = AsyncMock(return_value=None)
    repo.set_blocked_nodes_cached = AsyncMock(return_value=None)
    repo.get_node_capacity_cached = AsyncMock(return_value=None)
    return repo


@pytest.fixture
def mock_neo4j_repo() -> MagicMock:
    repo = MagicMock()
    repo.fetch_order_live_tracking = AsyncMock(
        return_value=GraphOrderTrackingStatusModel(
            order_id="ORD_0000042",
            product_id="PRD_0000105",
            current_logistic_stage=5,
            calculated_delay_minutes=52.0,
            is_bottleneck_flag=1,
            failing_node_id="HUB_001",
            bottleneck_risk_factor=0.89,
        )
    )
    repo.fetch_node_health_metrics = AsyncMock(
        return_value=GraphNodeHealthModel(
            node_id="HUB_001",
            status="CONGESTED",
            is_bottleneck_flag=1,
            bottleneck_risk_factor=0.89,
            affected_personnel_count=43,
        )
    )
    return repo


@pytest_asyncio.fixture
async def api_client(
    mock_pg_repo: MagicMock,
    mock_redis_repo: MagicMock,
    mock_neo4j_repo: MagicMock,
) -> AsyncGenerator[AsyncClient, None]:
    async def _override_session():
        yield AsyncMock()

    app.dependency_overrides[get_master_session] = _override_session
    app.dependency_overrides[get_replica_session] = _override_session
    app.dependency_overrides[get_postgres_repository] = lambda: mock_pg_repo
    app.dependency_overrides[get_redis_repository] = lambda: mock_redis_repo
    app.dependency_overrides[get_neo4j_repository] = lambda: mock_neo4j_repo

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client

    app.dependency_overrides.clear()
