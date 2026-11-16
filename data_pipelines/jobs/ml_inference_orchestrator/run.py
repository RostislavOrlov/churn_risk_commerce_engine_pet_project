import sys
import os
import re
import argparse
import logging
import signal
import uuid
import gc
from datetime import datetime, timezone

from data_pipelines.shared.config import GlobalSettings, settings
from data_pipelines.shared.connections import infra
from data_pipelines.jobs.ml_inference_orchestrator.pipeline import CentralMLInferencePipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("RealTimeCommerce.MLInferenceOrchestrator.Entrypoint")
SHUTDOWN_SIGNAL_RECEIVED = False


def system_signal_handler(signum, frame):
    global SHUTDOWN_SIGNAL_RECEIVED
    logger.warning(f"Получен системный сигнал остановки {signal.Signals(signum).name}. Инициируется безопасный выход...")
    SHUTDOWN_SIGNAL_RECEIVED = True


signal.signal(signal.SIGTERM, system_signal_handler)
signal.signal(signal.SIGINT, system_signal_handler)


def parse_target_date() -> str:
    parser = argparse.ArgumentParser(description="Central ML Inference Orchestrator")
    parser.add_argument("--target-date", type=str, default=None, help="Целевая дата YYYY-MM-DD")
    args, unknown = parser.parse_known_args()

    if args.target_date:
        return args.target_date
    if unknown and re.match(r"^\d{4}-\d{2}-\d{2}$", unknown):
        return unknown
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def bootstrap_and_execute() -> None:
    execution_id = str(uuid.uuid4())
    logger.info(
        f"ЗАПУСК ML INFERENCE\n"
        f"[SYS_INFO] Execution ID: {execution_id}\n[PID]: {os.getpid()}"
    )

    try:
        global_settings = settings or GlobalSettings()

        execution_date = parse_target_date()
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", execution_date):
            logger.critical(f"Критический сбой: неверный формат target_date '{execution_date}'. Ожидается YYYY-MM-DD.")
            sys.exit(1)

        logger.info(f"Целевая дата запуска сквозного ансамблирования и инференса: {execution_date}")

        pipeline = CentralMLInferencePipeline(global_settings)

        if SHUTDOWN_SIGNAL_RECEIVED:
            raise InterruptedError("Процесс остановлен сигналом ОС до начала выполнения ML-инференса.")

        pipeline.execute_sync(target_date=execution_date)
        logger.info("Процесс ML-инференса успешно завершил работу. Корректный выход.")
        sys.exit(0)

    except Exception as e:
        logger.critical(f"Краш на этапе сборки DI или рантайма воркера Спарка: {str(e)}", exc_info=True)
        sys.exit(1)

    finally:
        logger.info("Очистка сокетов: Закрытие активных пулов соединений ML-контура...")
        try:
            if hasattr(infra, "close"):
                infra.close()
        except Exception as close_err:
            logger.error(f"Ошибка при закрытии пулов инфраструктуры: {str(close_err)}")
        gc.collect()


if __name__ == "__main__":
    bootstrap_and_execute()
