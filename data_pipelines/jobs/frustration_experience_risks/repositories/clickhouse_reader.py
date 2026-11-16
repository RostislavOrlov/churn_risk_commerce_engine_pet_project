import sys
import argparse
import logging
from typing import Any
from data_pipelines.shared.config import GlobalSettings, settings
from data_pipelines.shared.connections import infra

logger = logging.getLogger("RealTimeCommerce.ClickHouseReader")


class ClickHouseReader:

    def __init__(self, settings: GlobalSettings = settings, ch_client: Any = None) -> None:
        
        self.settings = settings
        self.client = ch_client or infra.get_clickhouse_client()

    def get_jdbc_url(self) -> str:
        
        ch_cfg = getattr(self.settings, "clickhouse_config", {})
        host = ch_cfg.get("host") or getattr(self.settings, "clickhouse_host", "clickhouse")
        port = ch_cfg.get("port") or getattr(self.settings, "clickhouse_port", 8123)
        database = ch_cfg.get("database") or getattr(self.settings, "clickhouse_database", "default")
        user = ch_cfg.get("username") or getattr(self.settings, "clickhouse_user", "default")
        password = ch_cfg.get("password") or getattr(self.settings, "clickhouse_password", "")

        jdbc_url = (
            f"jdbc:clickhouse://{host}:{port}/{database}"
            f"?user={user}"
            f"&password={password}"
            f"&socket_timeout=60000"        
            f"&connection_timeout=10000"   
            f"&buffer_size=65536"           
        )

        logger.info(f"Успешно сформирован ClickHouse JDBC URL для Spark-сессии. Целевой хост: {host}:{port}")
        return jdbc_url

    def execute_native_flush(self) -> None:
        if not self.client:
            self.client = infra.get_clickhouse_client()
            
        logger.info("Отправка нативных команд сброса буферов ClickHouse (ASYNC INSERT QUEUE + LOGS)...")
        self.client.command("SYSTEM FLUSH ASYNC INSERT QUEUE")
        self.client.command("SYSTEM FLUSH LOGS")
        logger.info("Буферы оперативной памяти кликстрима успешно сброшены на диск. Данные готовы для Spark.")


def main():
    parser = argparse.ArgumentParser(description="Clickstream ClickHouse Ingestion Gateway Command Line Tool")
    parser.add_argument(
        "--action",
        type=str,
        required=True,
        choices=["flush"],
        help="Режим выполнения команды. Для Airflow используйте '--action flush'"
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, 
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    
    if args.action == "flush":
        logger.info("Инициализация планового сброса кликстрим-микробатчей (Flush Action)")
        
        try:
            ch_client = infra.get_clickhouse_client()
            repo = ClickHouseReader(settings=settings, ch_client=ch_client)
            repo.execute_native_flush()
            
            ch_client.close()
            sys.exit(0)
            
        except Exception as e:
            logger.error(f"Критический сбой физического сброса буферов кликстрима ClickHouse: {e}", exc_info=True)
            sys.exit(1)


if __name__ == "__main__":
    main()
