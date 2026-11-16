import os
import uuid
import random
import logging
from locust import FastHttpUser, task, between, events

logger = logging.getLogger("rtc_load_test")

BEARER_TOKEN = os.getenv("LOADTEST_BEARER_TOKEN", "")

import time
import threading
import urllib.request
import json

class AutoTokenManager:
    def __init__(self) -> None:
        self._token: str = BEARER_TOKEN
        self._expires_at: float = time.time() + 600 if BEARER_TOKEN else 0.0
        self._lock = threading.Lock()

    def get_token(self, base_url: str) -> str:
        if self._token and time.time() < self._expires_at:
            return self._token
        with self._lock:
            if self._token and time.time() < self._expires_at:
                return self._token
            req = urllib.request.Request(
                f"{base_url.rstrip('/')}/api/v1/auth/login",
                data=json.dumps({"username": "dispatcher_ivanov", "password": "password123"}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                self._token = data["access_token"]
                self._expires_at = time.time() + max(60, int(data.get("expires_in", 900)) - 120)
                logger.info("[LOCUST] Автоматически получен свежий JWT токен от Keycloak.")
                return self._token

token_manager = AutoTokenManager()


USER_POOL_SIZE = int(os.getenv("LOADTEST_USER_POOL", "5000"))
NODE_IDS = ["HUB_001", "HUB_002", "HUB_003", "HUB_004", "HUB_005", "DS_010", "DS_011", "WH_01"]
SAMPLE_INCIDENT_IDS = [
    os.getenv("LOADTEST_INCIDENT_ID", "11111111-2222-3333-4444-555555555555"),
    "22222222-3333-4444-5555-666666666666",
]


class ControlTowerTrafficUser(FastHttpUser):

    wait_time = between(0.05, 0.25)

    _shared_token: str = ""
    _token_lock = threading.Lock()

    def on_start(self) -> None:
        self.base_headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if not ControlTowerTrafficUser._shared_token:
            with ControlTowerTrafficUser._token_lock:
                if not ControlTowerTrafficUser._shared_token:
                    resp = self.client.post(
                        "/api/v1/auth/login",
                        json={"username": "dispatcher_ivanov", "password": "password123"},
                        name="POST /api/v1/auth/login (setup)",
                    )
                    if resp.status_code == 200:
                        ControlTowerTrafficUser._shared_token = resp.json()["access_token"]
                        logger.info("[LOCUST] Успешный авто-логин через self.client!")
                    else:
                        logger.error(f"[LOCUST] Ошибка авто-логина: {resp.status_code} {resp.text}")

        if ControlTowerTrafficUser._shared_token:
            self.base_headers["Authorization"] = f"Bearer {ControlTowerTrafficUser._shared_token}"


    def _random_user_and_order(self) -> tuple[str, str]:
        idx = random.randint(1, USER_POOL_SIZE)
        return f"USR_{idx:07d}", f"ORD_{idx:07d}"

    @task(42)
    def post_churn_risk_inference(self) -> None:
        """Эндпоинт 1 (42% трафика): Рантайм-инференс CatBoost + Action Engine."""
        user_id, order_id = self._random_user_and_order()
        headers = {
            **self.base_headers,
            "X-Request-ID": str(uuid.uuid4()),
            "X-Idempotency-Key": f"idem-{order_id}-{random.randint(1, 100000)}",
        }
        payload = {"user_id": user_id, "order_id": order_id}

        with self.client.post(
            "/api/v1/inference/churn-risk",
            json=payload,
            headers=headers,
            name="POST /api/v1/inference/churn-risk",
            catch_response=True,
        ) as resp:
            if resp.status_code == 200:
                resp.success()
            elif resp.status_code == 503 and resp.headers.get("Retry-After"):
                resp.failure("LOAD_SHEDDING_503")
            else:
                resp.failure(f"Unexpected status {resp.status_code}: {resp.text[:120]}")

    @task(15)
    def get_topology_capacities(self) -> None:
        """Эндпоинт 5 (15% трафика): Поллинг заблокированных узлов Online Routing Engine."""
        headers = {**self.base_headers, "X-Request-ID": str(uuid.uuid4())}
        with self.client.get(
            "/api/v1/topology/nodes/capacities",
            headers=headers,
            name="GET /api/v1/topology/nodes/capacities",
            catch_response=True,
        ) as resp:
            if resp.status_code == 200:
                resp.success()
            else:
                resp.failure(f"Status {resp.status_code}")

    @task(8)
    def get_customer_compensations(self) -> None:
        """Эндпоинт 2 (8% трафика): Карточка клиента в CRM колл-центра."""
        user_id, _ = self._random_user_and_order()
        headers = {**self.base_headers, "X-Request-ID": str(uuid.uuid4())}
        with self.client.get(
            f"/api/v1/customer/{user_id}/compensations",
            headers=headers,
            name="GET /api/v1/customer/{user_id}/compensations",
            catch_response=True,
        ) as resp:
            if resp.status_code == 200:
                resp.success()
            else:
                resp.failure(f"Status {resp.status_code}")

    @task(5)
    def get_active_incidents(self) -> None:
        """Эндпоинт 3 (5% трафика): Тепловая карта инцидентов Ситуационного центра."""
        headers = {**self.base_headers, "X-Request-ID": str(uuid.uuid4())}
        with self.client.get(
            "/api/v1/logistics/incidents/active?limit=50",
            headers=headers,
            name="GET /api/v1/logistics/incidents/active",
            catch_response=True,
        ) as resp:
            if resp.status_code == 200:
                resp.success()
            else:
                resp.failure(f"Status {resp.status_code}")

    @task(5)
    def get_node_health_passport(self) -> None:
        """Эндпоинт 6 (5% трафика): Паспорт здоровья узла и Z-Score."""
        node_id = random.choice(NODE_IDS)
        headers = {**self.base_headers, "X-Request-ID": str(uuid.uuid4())}
        with self.client.get(
            f"/api/v1/topology/nodes/{node_id}/health-passport",
            headers=headers,
            name="GET /api/v1/topology/nodes/{node_id}/health-passport",
            catch_response=True,
        ) as resp:
            if resp.status_code in (200, 404):
                resp.success()
            else:
                resp.failure(f"Status {resp.status_code}")

    @task(25)
    def post_incident_reroute(self) -> None:
        """Эндпоинт 4 (25% трафика): Перемаршрутизация инцидента диспетчером."""
        incident_id = random.choice(SAMPLE_INCIDENT_IDS)
        headers = {
            **self.base_headers,
            "X-Request-ID": str(uuid.uuid4()),
            "X-Idempotency-Key": f"reroute-{uuid.uuid4()}",
        }
        payload = {
            "assigned_team": "DISPATCH_NIGHT_SHIFT",
            "bypass_hub_id": "HUB_005",
            "reason": "Automated load test reroute trigger",
        }
        with self.client.post(
            f"/api/v1/logistics/incidents/{incident_id}/reroute",
            json=payload,
            headers=headers,
            name="POST /api/v1/logistics/incidents/{incident_id}/reroute",
            catch_response=True,
        ) as resp:
            if resp.status_code in (202, 404):
                resp.success()
            else:
                resp.failure(f"Status {resp.status_code}: {resp.text[:120]}")


@events.quitting.add_listener
def check_sla_thresholds(environment, **kwargs) -> None:
    stats = environment.stats.total
    p95_ms = stats.get_response_time_percentile(0.95) or 0.0
    p99_ms = stats.get_response_time_percentile(0.99) or 0.0
    fail_ratio = stats.fail_ratio * 100.0
    rps = stats.total_rps

    logger.info(
        f"\n================ SLA BENCHMARK REPORT ================\n"
        f"Total Requests : {stats.num_requests}\n"
        f"Throughput     : {rps:.1f} RPS\n"
        f"Latency p95    : {p95_ms:.2f} ms (SLA target: <= 50.0 ms)\n"
        f"Latency p99    : {p99_ms:.2f} ms (SLA target: <= 120.0 ms)\n"
        f"Failure Rate   : {fail_ratio:.2f}% (SLA target: <= 0.50%)\n"
        f"======================================================"
    )

    if p95_ms > 50.0 or fail_ratio > 0.5:
        logger.error("[SLA_VIOLATION] Целевые пороги производительности не выполнены!")
