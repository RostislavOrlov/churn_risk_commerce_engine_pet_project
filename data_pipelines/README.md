# Аналитический контур (`data_pipelines`)

Аналитический контур обработки данных платформы. Отвечает за приём событий в **ClickHouse**, предварительную фильтрацию через **dbt-core**, графовый поиск заторов в **Neo4j**, расчёт признаков на **Apache Spark** с предварительными проверками качества данных, сборку витрин в **PostgreSQL + TimescaleDB** и загрузку признаков в **Redis Online Feature Store**.

---

## Архитектура и инженерные паттерны

* **Потоковый приём и партиционированное хранение (Kafka Engine & MergeTree):**
  * Сырые потоки (`user_clicks`, `web_performance_logs`, `search_funnel_logs`, `clickstream_scan_events`) принимаются из **Apache Kafka** через движок `Kafka` и материализованные представления (`Materialized View`) в таблицы семейства `MergeTree` с партиционированием по датам (`log_date`).
  * Перед стартом вычислений инжекторы выполняют принудительный сброс асинхронных буферов ClickHouse на диск (`SYSTEM FLUSH ASYNC INSERT QUEUE`).
* **Предварительная фильтрация на стороне СУБД (Query Pushdown / dbt-core):**
  * Модели `dbt-core` (`stg_user_clicks`, `stg_web_performance`, `stg_search_funnel`, `stg_scan_events`) компилируют инкрементальные SQL-запросы с фильтрацией по целевой дате (`target_date`), чтобы **Apache Spark** вычитывал по JDBC только нужный суточный срез, а не всю историю.
* **Графовое моделирование и управление памятью (Graph Data Science & RAM Lifecycle Management):**
  * Пайплайн `logistics_graph_refresh` загружает инкремент физических сканирований (`ScanEvent`) в **Neo4j** через `neo4j-connector-apache-spark` (с приведением типов `DecimalType` к `Long`/`Double` и построением транзитных связей `[:TRANSIT]`).
  * В **Neo4j Graph Data Science** разворачивается временная проекция графа в оперативной памяти (`gds.graph.project`), алгоритм выделения сообществ **Лейдена** (`gds.leiden.stream`) локализует перегруженные хабы, а Cypher-запросы пересчитывают задержки доставки с учётом загруженности узлов и графиков работы ворот складов.
  * В блоке `finally` проекция гарантированно выгружается из RAM (`gds.graph.drop`), исключая утечки памяти в куче JVM.
* **Распределённый Feature Engineering и обогащение (Spark Catalyst & Painless Scripts):**
  * Задача `frustration_experience_risks` разбивает клики на сессии (порог неактивности 30 минут, отсечка незавершённых сессий `max_user_timestamp < now() - 30 min`) и считает 16 поведенческих признаков раздражения (частые клики < 250 мс, возвраты `catalog → product_card → catalog`, задержки рендеринга, ошибки API 5xx).
  * Данные обогащаются графовыми маршрутами из **Neo4j** и промо-каталогом **Elasticsearch 8** (с динамическим расчётом актуальных скидок через скрипты **Painless** и резервным скроллингом через Scroll API).
* **Многоступенчатые барьеры качества данных (Data Quality Gate & 4-Sigma Anomaly Detection):**
  * Перед записью в хранилище классы `DataQualityGate` и `LogisticsDataQualityGate` на **Apache Spark** проверяют датафреймы на пустоту, дубликаты (`event_id` ≤ 1%, телеметрия ≤ 2%), хронологическую целостность перемещения коробок (≤ 3%), ссылочную целостность товаров с каталогом **Elasticsearch** (≤ 10%) и статистические выбросы скорости загрузки страниц по правилу **4 сигм** (`avg + 4σ`). При нарушении любого порога расчёт аварийно останавливается, защищая боевые витрины.
* **Временные ряды, идемпотентность и двойной режим ML-скоринга (TimescaleDB Hypertables & Arrow UDF):**
  * Промежуточные буферы и история прогнозов (`ml_user_churn_scores`) хранятся в гипертаблицах **PostgreSQL 15 + TimescaleDB** с автоматическим сжатием старых чанков (`timescaledb.compress`) и идемпотентной перезаписью суточных партиций (`DELETE` по `log_date` / `ON CONFLICT DO UPDATE`).
  * Центральный оркестратор `ml_inference_orchestrator` объединяет UX- и логистические признаки в памяти Spark (`FULL OUTER JOIN`), запускает векторизированный инференс **CatBoost** через Apache Arrow (`@pandas_udf`) или переключается на нативный JVM-расчёт **Spark Catalyst** при отсутствии файла модели, после чего строит витрины инцидентов, компенсаций и лимитов хабов.
* **Пакетный прогрев онлайн-слоя и оркестрация без ручных действий (Redis Pipeline & Self-Healing Bootstrap DAG):**
  * Скрипт `redis_sync.py` через `Redis Pipeline` загружает векторы признаков (`online:features:user:{id}`), лимиты пропускной способности хабов и активные инциденты в **Redis**, а также отправляет heartbeat-метрики в **Prometheus Pushgateway**.
  * Оркестратор **`master_bootstrap_pipeline`** в **Apache Airflow** автоматически снимает дочерние цепочки с паузы, запускает их по графу зависимостей с единым `execution_date` и возвращает на паузу (`trigger_rule="all_done"`) после завершения.

---

## Граф оркестрации (Apache Airflow)

Сквозной прогрев всего аналитического контура запускается одной точкой входа — **`master_bootstrap_pipeline`**:

```text
master_bootstrap_pipeline
 ├── t0: unpause_child_dags
 ├── t1: [clickstream_collector_pipeline, wms_collector_pipeline]
 ├── t2: logistics_graph_refresh_pipeline
 ├── t3: [stochastic_logistics_tracking_pipeline, frustration_experience_risks]
 ├── t4: comprehensive_churn_decision_ensemble (Marts + Redis Sync + Pushgateway)
 └── t5: pause_child_dags
```

---

## Структура директории

```text
data_pipelines/
├── airflow/dags/
│   ├── business_domain/     # Бизнес-пайплайны UX-фрустрации и стохастической логистики
│   └── infrastructure/      # Инжекторы буферов, обновление графа Neo4j, ансамбль витрин и Master DAG
├── database/
│   ├── migrations/          # DDL-миграции ClickHouse, PostgreSQL/TimescaleDB, Neo4j, Elasticsearch
│   ├── seeds/               # Генератор синтетических данных и оркестратор нативного bulk-импорта
│   └── sql_marts/           # SQL-скрипты консолидации буферов и расчёта витрин dw_marts
├── dbt/                     # Конфигурация dbt-clickhouse и эфемерные staging-модели
├── deploy/                  # Docker Compose инфраструктуры, конфиги PostgreSQL и Prometheus
├── jobs/                    # Задачи Apache Spark, Cypher-запросы GDS, Data Quality Gate и Redis Sync
├── shared/                  # Единая конфигурация Pydantic (GlobalSettings) и пулы подключений
└── README.md
```
