import uuid
from unittest.mock import patch, MagicMock
import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_health_and_readiness_probes(api_client: AsyncClient):
    res_health = await api_client.get("/healthz")
    assert res_health.status_code == 200
    assert res_health.json()["status"] == "UP"

    res_ready = await api_client.get("/readyz")
    assert res_ready.status_code == 200
    assert res_ready.json()["status"] == "READY"


@pytest.mark.asyncio
async def test_endpoint_1_post_churn_risk_vip_voucher(api_client: AsyncClient):
    response = await api_client.post(
        "/api/v1/inference/churn-risk",
        json={"user_id": "USR_0000042", "order_id": "ORD_0000042"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["user_id"] == "USR_0000042"
    assert body["is_high_risk"] is True
    assert body["action_taken"] == "PREEMPTIVE_VOUCHER_ISSUED"
    assert body["voucher_code"].startswith("CARE-")
    assert body["discount_amount_rub"] == 400.0


@pytest.mark.asyncio
async def test_endpoint_1_graceful_degradation_when_redis_down(
    api_client: AsyncClient, mock_redis_repo: MagicMock
):
    mock_redis_repo.get_user_online_features.side_effect = ConnectionError("Redis cluster unreachable")

    response = await api_client.post(
        "/api/v1/inference/churn-risk",
        json={"user_id": "USR_0000042", "order_id": "ORD_0000042"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["degraded_mode"] is True
    assert 0.0 <= body["churn_probability"] <= 1.0


@pytest.mark.asyncio
async def test_endpoint_2_get_customer_compensations(api_client: AsyncClient):
    response = await api_client.get("/api/v1/customer/USR_0000042/compensations")
    assert response.status_code == 200
    body = response.json()
    assert body["user_id"] == "USR_0000042"
    assert body["total_count"] == 1
    assert body["items"][0]["voucher_code"] == "CARE-ABCD1234"


@pytest.mark.asyncio
async def test_endpoint_3_get_active_incidents_fallback(api_client: AsyncClient):
    response = await api_client.get("/api/v1/logistics/incidents/active")
    assert response.status_code == 200
    body = response.json()
    assert body["source"] == "POSTGRES_REPLICA_FALLBACK"
    assert body["total_count"] == 1
    assert body["incidents"][0]["failing_node_id"] == "HUB_001"


@pytest.mark.asyncio
async def test_endpoint_4_post_incident_reroute(
    api_client: AsyncClient, sample_incident_id: uuid.UUID
):
    fake_task = MagicMock()
    fake_task.id = "celery-task-999"

    with patch("src.services.logistics_ops.dispatch_reroute_command_tms.delay", return_value=fake_task) as mock_delay:
        response = await api_client.post(
            f"/api/v1/logistics/incidents/{sample_incident_id}/reroute",
            json={"assigned_team": "DISPATCH_NIGHT_SHIFT", "bypass_hub_id": "HUB_005"},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["new_status"] == "REROUTED"
    assert body["tms_task_id"] == "celery-task-999"
    mock_delay.assert_called_once()


@pytest.mark.asyncio
async def test_endpoint_5_get_topology_capacities(api_client: AsyncClient):
    response = await api_client.get("/api/v1/topology/nodes/capacities")
    assert response.status_code == 200
    body = response.json()
    assert body["total_restricted_nodes"] == 1
    assert body["nodes"][0]["node_id"] == "HUB_001"


@pytest.mark.asyncio
async def test_endpoint_6_get_node_health_passport(api_client: AsyncClient):
    response = await api_client.get("/api/v1/topology/nodes/HUB_001/health-passport")
    assert response.status_code == 200
    body = response.json()
    assert body["node_id"] == "HUB_001"
    assert body["affected_personnel_count"] == 43
    assert body["health_grade"] == "CRITICAL_BLOCKED"
    assert body["performance_lag_zscore"] >= 3.0
