import os
import sys
import gc
import logging
import multiprocessing

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(process)d] [%(levelname)s] %(message)s")

bind = f"{os.getenv('HTTP_HOST', '0.0.0.0')}:{os.getenv('HTTP_PORT', '8080')}"
workers = int(os.getenv("WORKERS_COUNT", multiprocessing.cpu_count() * 2 + 1))
worker_class = "uvicorn.workers.UvicornWorker"

timeout = 30
keepalive = 5
max_requests = 10000
max_requests_jitter = 1000
backlog = 2048
preload_app = False

accesslog = "-"
errorlog = "-"
loglevel = os.getenv("LOG_LEVEL", "info").lower()

def on_starting(server):
    server.log.info("=== Starting Gunicorn Master Process: RealTimeCommerce Platform ===")

def post_fork(server, worker):
    server.log.info(f"Worker spawned successfully with PID: {worker.pid}")
    gc.disable()
    gc.collect()
    gc.enable()

def worker_exit(server, worker):
    server.log.warning(f"Worker exiting (Max requests reached or timeout) with PID: {worker.pid}. Cleaning process heap...")
    gc.collect()
    sys.stdout.flush()
    sys.stderr.flush()
