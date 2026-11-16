# Real-Time Commerce & Logistics Control Tower — Online Contour (`backend-online-contour`)

Высокопроизводительный асинхронный микросервис реального времени (Speed Layer) на базе **FastAPI**, **SQLAlchemy 2.0 (asyncpg)**, **Redis**, **Neo4j**, **CatBoost** и **Celery**. Отвечает за рантайм-прогнозирование оттока клиентов, проактивное удержание (Action Engine) и оперативное управление сбоями логистической топологии.

---

## Архитектурные особенности

* **Read/Write Splitting (PostgreSQL):** Чтение аналитических витрин (`dwh_user_ltv`, `dwh_market_margins`, `incident_log`, `node_dynamic_capacities`) направляется на реплику (`postgres-replica-1:5433`), а транзакционная запись ваучеров и статусов инцидентов — строго на мастер (`postgres-master:5432`) с защитой от гонок (`ON CONFLICT DO NOTHING`).
* **Low-Latency ML Serving (CatBoost):** Модель `user_churn_retail_v2.cbm` прогревается в ОЗУ при старте сервиса. Сбор 34 признаков (19 UX/каталожных из Redis HASH + 12 логистических из Neo4j + 3 синтетических метрики фрустрации) выполняется параллельно через `asyncio.gather()`, а вызов C++ ядра `predict_proba` вынесен в `asyncio.to_thread()`.
* **Отказоустойчивость (Circuit Breaker & Graceful Degradation):** Изоляция сбойных контуров Redis, Neo4j и PostgreSQL с автоматическим переходом на безопасные дефолтные векторы признаков без падения HTTP-запросов.
* **Идемпотентность и безопасность:** Поддержка заголовка `Idempotency-Key` (через Redis `SET NX`), M2M-авторизация через Keycloak JWKS с проверкой ролей (RBAC) и K8s-совместимый 4-фазный Graceful Shutdown.

---

## API Эндпоинты (`/api/v1`)

| Метод | Путь | Роли RBAC | Описание |
| :--- | :--- | :--- | :--- |
| `POST` | `/api/v1/inference/churn-risk` | `OPERATOR`, `DISPATCHER` | Параллельный сбор фичей (Redis + Neo4j), инференс CatBoost (34 фичи) и выдача ваучера удержания в `postgres-master`. |
| `GET` | `/api/v1/customer/{user_id}/compensations` | `OPERATOR`, `DISPATCHER` | История начисленных клиенту компенсаций и статусов отправки с `postgres-replica`. |
| `GET` | `/api/v1/logistics/incidents/active` | `DISPATCHER`, `CRITICAL_OPS` | Список активных инцидентов (`RAISED`, `REROUTED`) из кэша `online:incidents:active` с фолбэком на реплику. |
| `POST` | `/api/v1/logistics/incidents/{incident_id}/reroute` | `DISPATCHER`, `CRITICAL_OPS` | Перевод инцидента в статус `REROUTED` в `master`, инвалидация кэша и постановка задачи в Celery для TMS. |
| `GET` | `/api/v1/topology/nodes/capacities` | `ROUTING_ENGINE`, `DISPATCHER` | Список заблокированных и рискованных хабов из `online:topology:blocked_nodes` для Online Routing Engine. |
| `GET` | `/api/v1/topology/nodes/{node_id}/health-passport` | `DISPATCHER`, `CRITICAL_OPS` | Агрегация емкости хаба (Postgres/Redis), пострадавшего персонала (Neo4j) и расчет Z-Score лага. |

Служебные эндпоинты: `GET /healthz` (Liveness), `GET /readyz` (Readiness), `GET /metrics` (Prometheus).

---

## Быстрый старт (локально)

### 1. Установка зависимостей
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### 2. Запуск в режиме разработки (без Keycloak)
```bash
make dev
```
Документация Swagger будет доступна по адресу `http://localhost:8080/docs`.

### 3. Запуск фонового воркера Celery
```bash
make celery-worker
```

### 4. Запуск автотестов
```bash
make test
```

---

## Сборка и деплой

### Docker
```bash
docker build -t backend-online-contour:1.0.0 .
docker run -p 8080:8080 --env-file .env backend-online-contour:1.0.0
```

### Kubernetes
```bash
kubectl apply -f deployment.yaml
```
