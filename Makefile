.PHONY: up down restart ps logs-bootstrap rebuild-api test load-test clean

COMPOSE := docker compose -f data_pipelines/deploy/docker-compose.yml -f apps/backend-online-contour/docker-compose.online.yml

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

clean:
	$(COMPOSE) down -v --remove-orphans

restart:
	$(COMPOSE) restart

ps:
	$(COMPOSE) ps

logs-bootstrap:
	docker logs -f retail_airflow_dag_bootstrap

rebuild-api:
	$(COMPOSE) up -d --build backend-online-contour

test:
	cd apps/backend-online-contour && pytest -v

load-test:
	cd apps/backend-online-contour && locust -f load_tests/locustfile.py --host=http://127.0.0.1:8000 --web-port 8095
