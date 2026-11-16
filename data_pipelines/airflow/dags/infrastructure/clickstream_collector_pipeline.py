from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import DagRun

default_args = {
    'owner': 'telemetry_infrastructure_team',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 5,        
    'retry_delay': timedelta(seconds=30),
    'execution_timeout': timedelta(minutes=15)
}

with DAG(
    dag_id='clickstream_collector_pipeline',
    default_args=default_args,
    schedule_interval=None,      
    start_date=datetime(2026, 11, 9),
    catchup=False,   
    max_active_runs=1,      
    tags=['ingestion', 'kafka', 'clickhouse', 'telemetry'],
) as dag:

    flush_buffer_to_olap = BashOperator(
        task_id="flush_kafka_microbatches_to_clickhouse",
        cwd="/opt/airflow",
        env={
            "PYTHONPATH": "/opt/airflow",
        },
        append_env=True,
        bash_command="python -m data_pipelines.jobs.frustration_experience_risks.repositories.clickhouse_reader --action flush",
    )


    flush_buffer_to_olap
