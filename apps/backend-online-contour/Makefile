.PHONY: run dev celery-worker test

run:
	gunicorn -c config/gunicorn_config.py src.main:app

dev:
	AUTH_ENABLED=False uvicorn src.main:app --host 0.0.0.0 --port 8080 --reload

celery-worker:
	celery -A src.tasks.celery_app worker --loglevel=info -Q default,critical_ops

test:
	pytest -v
