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
from data_pipelines.jobs.stochastic_logistics_tracking.pipeline import StochasticLogisticsTrackingPipeline
from data_pipelines.jobs.stochastic_logistics_tracking.feature_generator import StochasticLogisticsFeatureGenerator

from data_pipelines.jobs.stochastic_logistics_tracking.repositories.neo4j_logistics import Neo4jLogisticsRepository
from data_pipelines.jobs.stochastic_logistics_tracking.repositories.postgres_writer import PostgresWriterRepository


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("RealTimeCommerce.StochasticLogisticsTracking.Entrypoint")
SHUTDOWN_SIGNAL_RECEIVED = False


def system_signal_handler(signum, frame):
    global SHUTDOWN_SIGNAL_RECEIVED
    logger.warning(f"Получен системный сигнал {signal.Signals(signum).name}. Инициируется безопасная остановка...")
    SHUTDOWN_SIGNAL_RECEIVED = True


signal.signal(signal.SIGTERM, system_signal_handler)
signal.signal(signal.SIGINT, system_signal_handler)


def parse_target_date() -> str:
    parser = argparse.ArgumentParser(description="Stochastic Logistics Tracking Spark Job")
    parser.add_argument("--target-date", type=str, default=None, help="Целевая дата YYYY-MM-DD")
    args, unknown = parser.parse_known_args()

    if args.target_date:
        return args.target_date
    if unknown and re.match(r"^\d{4}-\d{2}-\d{2}$", unknown[0]):
        return unknown[0]
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def bootstrap_and_execute() -> None:
    execution_id = str(uuid.uuid4())
    logger.info(
        f"ЗАПУСК РАСПРЕДЕЛЕННОГО ЛОГИСТИЧЕСКОГО КОНТУРА\n"
        f"[SYS_INFO] Execution ID: {execution_id}\n[PID]: {os.getpid()}"
    )

    try:
        global_settings = settings or GlobalSettings()

        execution_date = parse_target_date()
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", execution_date):
            logger.critical(f"Критический сбой: неверный формат target_date '{execution_date}'. Ожидается YYYY-MM-DD.")
            sys.exit(1)

        logger.info(f"Целевая дата запуска распределенного логистического анализа: {execution_date}")

        logger.info("Выполнение сквозного Healthcheck и прогрева сетевых сокетов для BigData-контура...")
        ch_client = infra.get_clickhouse_client()
        ch_client.command("SELECT 1")

        neo4j_driver = infra.get_neo4j_driver()
        neo4j_driver.verify_connectivity()

        neo4j_gds_repo = Neo4jLogisticsRepository(settings=global_settings, neo4j_driver=neo4j_driver)
        postgres_repo = PostgresWriterRepository(settings=global_settings)

        feature_generator = StochasticLogisticsFeatureGenerator(global_settings)

        pipeline = StochasticLogisticsTrackingPipeline(
            settings=global_settings,
            neo4j_gds_repo=neo4j_gds_repo,
            postgres_repo=postgres_repo,
        )
        pipeline.feature_generator = feature_generator

        if SHUTDOWN_SIGNAL_RECEIVED:
            raise InterruptedError("Процесс остановлен сигналом ОС до начала выполнения Graph-ETL.")

        pipeline.execute_sync(target_date=execution_date)
        logger.info("Процесс распределенного логистического трекинга успешно завершил работу. Корректный выход.")
        sys.exit(0)

    except Exception as e:
        logger.critical(f"Краш на этапе сборки DI или рантайма воркера Спарка: {str(e)}", exc_info=True)
        sys.exit(1)

    finally:
        logger.info("Очистка сокетов: Закрытие активных пулов соединений логистического контура...")
        try:
            if hasattr(infra, "close"):
                infra.close()
            elif hasattr(infra, "_neo4j_driver") and infra._neo4j_driver:
                infra._neo4j_driver.close()
        except Exception as close_err:
            logger.error(f"Ошибка при закрытии пулов инфраструктуры: {str(close_err)}")
        gc.collect()



if __name__ == "__main__":
    bootstrap_and_execute()
