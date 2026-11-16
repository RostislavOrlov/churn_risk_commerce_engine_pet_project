import sys
import argparse
import logging
from datetime import datetime

from data_pipelines.shared.config import GlobalSettings

from data_pipelines.shared.connections import infra

from data_pipelines.jobs.frustration_experience_risks.pipeline import FrustrationExperienceRisksPipeline
from data_pipelines.jobs.frustration_experience_risks.feature_generator import FrustrationExperienceRisksFeatureGenerator

from data_pipelines.jobs.frustration_experience_risks.repositories.clickhouse_reader import ClickHouseReader
from data_pipelines.jobs.frustration_experience_risks.repositories.neo4j_logistics import Neo4jLogisticsRepository
from data_pipelines.jobs.frustration_experience_risks.repositories.elasticsearch_marketing import ElasticsearchMarketingRepository
from data_pipelines.jobs.frustration_experience_risks.repositories.postgres_writer import PostgresWriterRepository

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("RealTimeCommerce.FrustrationExperienceRisks.Run")


def parse_args():
    parser = argparse.ArgumentParser(description="UX & Operations Intelligence Spark Pipeline")
    parser.add_argument(
        "--target-date",
        type=str,
        required=True,
        help="Целевая дата расчета инкремента в формате YYYY-MM-DD (передается как {{ ds }} в Airflow)"
    )
    return parser.parse_args()


def main():
    start_time = datetime.now()
    args = parse_args()
    
    try:
        datetime.strptime(args.target_date, "%Y-%m-%d")
    except ValueError:
        logger.error(f"Неверный формат аргумента --target-date: '{args.target_date}'. Ожидается YYYY-MM-DD.")
        sys.exit(1)

    logger.info(f"=== Инициализация инфраструктурного конвейера для даты: {args.target_date} ===")
    
    settings = GlobalSettings()

    logger.info("Установление коннектов к гетерогенным СУБД...")
    ch_client = infra.get_clickhouse_client()
    neo4j_driver = infra.get_neo4j_driver()
    es_client = infra.get_elasticsearch_client()
    pg_pool = infra.get_pg_pool()

    try:
        ch_reader = ClickHouseReader(settings, ch_client=ch_client)
        neo4j_repo = Neo4jLogisticsRepository(settings, neo4j_driver=neo4j_driver)
        es_repo = ElasticsearchMarketingRepository(settings, es_client=es_client)
        postgres_repo = PostgresWriterRepository(settings, pool_provider=pg_pool)

        logger.info("Сборка вычислительного ядра пайплайна...")
        feature_generator = FrustrationExperienceRisksFeatureGenerator(settings)

        pipeline = FrustrationExperienceRisksPipeline(
            settings=settings,
            ch_reader=ch_reader,
            neo4j_repo=neo4j_repo,
            es_repo=es_repo,
            postgres_repo=postgres_repo
        )
        
        pipeline.feature_generator = feature_generator

        pipeline.execute_sync(target_date=args.target_date)
        
        execution_duration = datetime.now() - start_time
        logger.info(f"Процесс успешно завершен за {execution_duration.total_seconds():.2f} сек.")

    except Exception as e:
        logger.critical(f"Краш на этапе инициализации или выполнения процесса: {str(e)}", exc_info=True)
        sys.exit(1)
        
    finally:
        logger.info("Закрытие пулов соединений и инфраструктурных драйверов...")
        if 'neo4j_driver' in locals():
            neo4j_driver.close()
        if 'pg_pool' in locals():
            pg_pool.close()
        logger.info("=== Инфраструктурный контур полностью очищен. Выход. ===")


if __name__ == "__main__":
    main()
