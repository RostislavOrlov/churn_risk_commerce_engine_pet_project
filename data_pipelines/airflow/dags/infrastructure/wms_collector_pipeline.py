from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'supply_chain_infrastructure_team',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=20)
}

with DAG(
    dag_id='wms_collector_pipeline',
    default_args=default_args,
    schedule_interval=None, 
    start_date=datetime(2026, 11, 9),
    catchup=False, 
    max_active_runs=1,
    tags=['ingestion', 'kafka', 'clickhouse', 'wms', 'supply_chain'],
) as dag:

    flush_logistics_stream_to_olap = BashOperator(
        task_id="flush_logistics_events_to_clickhouse",
        cwd="/opt/airflow",
        env={
            "PYTHONPATH": "/opt/airflow",
        },
        append_env=True,
        bash_command="python data_pipelines/jobs/logistics_graph_refresh/repositories/clickhouse_event_stream.py --action flush",
    )

    flush_logistics_stream_to_olap
