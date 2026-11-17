import argparse
import logging
import sys
from datetime import datetime

from data_pipelines.shared.config import GlobalSettings, settings
from data_pipelines.shared.connections import infra

from data_pipelines.jobs.logistics_graph_refresh.repositories.clickhouse_event_stream import ClickHouseEventStreamRepository
from data_pipelines.jobs.logistics_graph_refresh.repositories.neo4j_bulk_importer import Neo4jBulkImporterRepository
from data_pipelines.jobs.logistics_graph_refresh.pipeline import IngestLogisticsStreamPipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s %(filename)s:%(lineno)d: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("RealTimeCommerce.IngestLogisticsStream.Main")


def parse_cli_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="High-Load ClickHouse-to-Neo4j Inbound CDC Ingestion Job"
    )
    parser.add_argument(
        "--target-date",
        type=str,
        required=True,
        help="Target operational execution date in YYYY-MM-DD format",
    )

    args, _ = parser.parse_known_args()

    try:
        datetime.strptime(args.target_date, "%Y-%m-%d")
    except ValueError as exc:
        logger.error("Критическая ошибка: Параметр --target-date должен строго соответствовать шаблону YYYY-MM-DD")
        raise ValueError(f"Invalid CLI argument format: {exc}") from exc

    return args


def main() -> None:
    logger.info("Инициализация инфраструктурного контекста Ingestion-слоя")

    try:
        args = parse_cli_arguments()
        global_settings = settings or GlobalSettings()

        ch_client = infra.get_clickhouse_client()
        neo4j_driver = infra.get_neo4j_driver()

        ch_stream_repo = ClickHouseEventStreamRepository(settings=global_settings, ch_client=ch_client)
        neo4j_bulk_repo = Neo4jBulkImporterRepository(settings=global_settings, neo4j_driver=neo4j_driver)

        pipeline = IngestLogisticsStreamPipeline(
            settings=global_settings,
            ch_stream_repo=ch_stream_repo,
            neo4j_bulk_repo=neo4j_bulk_repo,
        )

        pipeline.execute_ingest_sync(target_date=args.target_date)

        logger.info("Скрипт run.py успешно завершил выполнение без системных ошибок")
        sys.exit(0)

    except Exception as fatal_error:
        logger.critical(
            "Катастрофический сбой рантайма при инициализации процесса обновления графа Neo4j: %s",
            str(fatal_error),
            exc_info=True,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
