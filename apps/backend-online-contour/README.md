# Онлайн-контур реального времени (`backend-online-contour`)

Высокопроизводительный асинхронный микросервис реального времени на базе **FastAPI**, **SQLAlchemy 2.0 (`asyncpg`)**, **Redis**, **Neo4j**, **CatBoost** и **Celery**. Отвечает за рантайм-прогнозирование оттока клиентов, автоматическое начисление упреждающих компенсаций, оперативное управление сбоями логистики и потоковую трансляцию событий в Ситуационный центр.

---

## Архитектура и ключевые особенности

* **Разделение чтения и записи в PostgreSQL:** чтение аналитических витрин (`dwh_user_ltv`, `dwh_market_margins`, `incident_log`, `node_dynamic_capacities`) направляется на реплику (`postgres-replica-1`), а транзакционная запись ваучеров и статусов инцидентов — строго на мастер (`postgres-master`) с защитой от дублей (`ON CONFLICT DO NOTHING`).
* **ML-скоринг и теневое сравнение моделей (CatBoost):**
  * Основная модель `user_churn_retail_v2.cbm` загружается и прогревается в оперативной памяти при старте воркера (`warm_up`), чтобы первые запросы не имели задержек.
  * Сбор 34 признаков (19 поведенческих и каталожных из **Redis** + 12 графовых признаков маршрута из **Neo4j** + 3 расчётных индекса) выполняется параллельно через `asyncio.gather()`, а вызов C++ ядра `predict_proba` вынесен в пул потоков (`asyncio.to_thread()`), не блокируя цикл событий.
  * Теневая модель-кандидат (`user_churn_retail_challenger.cbm`) параллельно оценивает запросы в фоне и пишет расхождение прогнозов в **Prometheus**.
* **Двухуровневое кэширование и защита от сбоев:**
  * Гибридный кэш сочетает локальную оперативную память воркера (L1 с HMAC-подписью ключей) и **Redis** (L2).
  * Независимые предохранители (`CLOSED → HALF-OPEN → OPEN`) изолируют сбойные хранилища (**Redis**, **Neo4j**, **PostgreSQL**) и переводят сервис в режим плавной деградации с резервной формулой расчёта.
  * Pure ASGI middleware мгновенно отсекает избыточные запросы при перегрузке (`503 Retry-After: 1`).
* **Идемпотентность запросов (Idempotency Key):** повторные `POST`-запросы с заголовком `X-Idempotency-Key` в течение 24 часов возвращают сохранённый ответ из **Redis** (`SET NX`) и блокируют параллельные дубли статусом `409 Conflict`.
* **Безопасная остановка и изоляция воркеров (Graceful Shutdown & Process Isolation):** при остановке контейнера выполняется 4-фазный **Graceful Shutdown** (перевод `/readyz` в `503` → пауза вывода из балансировки → дренирование активных запросов и фоновых задач → закрытие пулов PostgreSQL, Redis и Neo4j). В **Gunicorn** отключён `preload_app = False`, чтобы пулы сокетов и C++ потоки CatBoost инициализировались изолированно в каждом процессе-воркере.
* **Потоковый CDC и гарантированная доставка в логистику:** изменения заказов из **PostgreSQL** через **Debezium** и **Kafka** в реальном времени обновляют граф **Neo4j**, а команды диспетчеров на перемаршрутизацию отправляются в фоне через **Celery** с нарастающими паузами и парковкой неудачных задач в очередь ошибок (`dead_letter`) и список в **Redis**.
* **Безопасность (Keycloak OIDC и RBAC):** проверка RS256 JWT-токенов через JWKS с автоматическим обновлением при смене ключей подписи, разграничение прав по ролям (`OPERATOR`, `DISPATCHER`, `CRITICAL_OPS`, `ROUTING_ENGINE`) и мгновенная блокировка токена при выходе пользователя через чёрный список в **Redis**.

---

## Эндпоинты REST API и WebSocket (`/api/v1`)

| Метод | Путь | Роли доступа | Назначение |
| :--- | :--- | :--- | :--- |
| `POST` | `/api/v1/auth/login` | Все | Вход пользователя через **Keycloak** и выдача пары `access_token` + `refresh_token`. |
| `POST` | `/api/v1/auth/refresh` | Все | Обновление пары токенов по `refresh_token`. |
| `POST` | `/api/v1/auth/logout` | Авторизованные | Завершение сессии и мгновенный отзыв токена через чёрный список в **Redis**. |
| `GET` | `/api/v1/auth/me` | Авторизованные | Получение профиля и активных ролей текущего пользователя. |
| `POST` | `/api/v1/inference/churn-risk` | `OPERATOR`, `DISPATCHER` | Параллельный сбор 34 признаков, скоринг **CatBoost** и начисление компенсации в `postgres-master`. |
| `GET` | `/api/v1/customer/{user_id}/compensations` | `OPERATOR`, `DISPATCHER` | История начисленных клиенту промокодов с `postgres-replica` (с кэшированием в **Redis**). |
| `GET` | `/api/v1/logistics/incidents/active` | `DISPATCHER`, `CRITICAL_OPS` | Список активных инцидентов (`RAISED`, `REROUTED`) из кэша **Redis** с фолбэком на реплику. |
| `POST` | `/api/v1/logistics/incidents/{incident_id}/reroute` | `DISPATCHER`, `CRITICAL_OPS` | Перевод инцидента в статус `REROUTED`, сброс кэша и постановка задачи в **Celery**. |
| `GET` | `/api/v1/topology/nodes/capacities` | `ROUTING_ENGINE`, `DISPATCHER` | Список заблокированных и перегруженных хабов из кэша L1/L2 для сервиса маршрутизации. |
| `GET` | `/api/v1/topology/nodes/{node_id}/health-passport` | `DISPATCHER`, `CRITICAL_OPS` | Агрегация текущей ёмкости склада (**PostgreSQL**), затронутого персонала (**Neo4j**) и расчёт Z-Score. |
| `WS` | `/api/v1/ws/control-tower` | Диспетчер UI | Потоковая передача срезов инцидентов и блокировок узлов каждые 3 секунды. |

Служебные эндпоинты: `GET /ui` (веб-консоль Ситуационного центра), `GET /healthz` (проверка живучести процесса), `GET /readyz` (проверка готовности к приёму трафика), `GET /metrics` (метрики **Prometheus**).

---

## Структура директории

```text
apps/backend-online-contour/
├── cdc/                       # Образ Debezium Connect и конфигурации коннекторов Postgres -> Neo4j
├── config/                    # Настройки Gunicorn (preload_app=False) и JSON-логирования
├── keycloak/                  # Экспорт настроек Keycloak (роли RBAC, клиенты, тестовые пользователи)
├── load_tests/                # Сценарий нагрузочного тестирования Locust
├── monitoring/                # Конфигурация Prometheus и авто-загрузка дашбордов Grafana
├── src/
│   ├── api/v1/                # Роутеры REST API и WebSocket-потока
│   ├── core/                  # Кэш L1/L2, предохранители, идемпотентность, OIDC-безопасность, метрики
│   ├── models/                # Модели таблиц SQLAlchemy и схемы кэша/графа Pydantic
│   ├── repositories/          # Слой доступа к PostgreSQL (Master/Replica), Redis и Neo4j
│   ├── services/              # Бизнес-логика: Action Engine, CatBoost Serving, Logistics Ops, Topology Health
│   ├── static/                # Веб-интерфейс Ситуационного центра (control_tower.html)
│   ├── tasks/                 # Очереди Celery, планировщик и обработка невыполненных задач (DLQ)
│   ├── config.py              # Типизированная конфигурация приложения (Pydantic Settings)
│   └── main.py                # Точка входа FastAPI, жизненный цикл ресурсов, Load Shedding и пробы
├── tests/                     # Модульные и интеграционные тесты (pytest)
├── docker-compose.online.yml  # Оркестрация контейнеров онлайн-контура
├── Dockerfile                 # Сборка образа сервиса
└── pyproject.toml             # Зависимости Python
```

---

## Локальный запуск и тестирование

### 1. Установка зависимостей для тестов и локального запуска
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### 2. Запуск в режиме разработки (без проверки токенов Keycloak)
```bash
AUTH_ENABLED=False uvicorn src.main:app --host 0.0.0.0 --port 8080 --reload
```
Веб-консоль Ситуационного центра будет доступна по адресу `http://localhost:8080/ui`.

### 3. Запуск фонового воркера Celery
```bash
celery -A src.tasks.celery_app worker --loglevel=info -Q default,critical_ops,dead_letter
```

### 4. Запуск автотестов и нагрузочного тестирования
```bash
pytest -v
locust -f load_tests/locustfile.py --host=http://127.0.0.1:8000 --web-port 8095
```

---

## Сборка и деплой

### Docker
```bash
docker build -t backend-online-contour:1.0.0 .
docker run -p 8080:8080 --env-file .env backend-online-contour:1.0.0
```
