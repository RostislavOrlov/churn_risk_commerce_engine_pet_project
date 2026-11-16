from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import DagRun
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


def get_most_recent_wms_run(dt, **kwargs):
    dag_runs = DagRun.find(dag_id="wms_collector_pipeline", state="success")
    if not dag_runs:
        return dt
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    return dag_runs[0].execution_date


default_args = {
    "owner": "senior_logistics_architecture_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="logistics_graph_refresh_pipeline",
    default_args=default_args,
    schedule_interval=None,  
    start_date=datetime(2026, 11, 9),
    catchup=False,   
    max_active_runs=1,
    tags=["production", "logistics", "neo4j", "clickhouse", "spark", "data_quality", "cdc"],
) as dag:

    wait_for_logistics_logs = ExternalTaskSensor(
        task_id="wait_for_wms_logistics_dump",
        external_dag_id="wms_collector_pipeline",
        external_task_id="flush_logistics_events_to_clickhouse",
        execution_date_fn=get_most_recent_wms_run,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        poke_interval=15,
        timeout=1800,
        mode="reschedule",
    )

    compile_dbt_scan_events_query = BashOperator(
        task_id="compile_dbt_scan_events_pushdown",
        cwd="/opt/airflow/data_pipelines/dbt",
        bash_command=(
            "dbt compile --profiles-dir . --vars '{\"target_date\": \"{{ dag_run.conf.get(\"target_date\") or \"2026-11-09\" }}\"}'"
        ),
    )

    run_spark_neo4j_graph_refresh = SparkSubmitOperator(
        task_id="run_spark_neo4j_scanevent_ingestion",
        application="/opt/airflow/data_pipelines/jobs/logistics_graph_refresh/run.py",
        conn_id="spark_default",
        conf={
            "spark.driver.host": "airflow",
            "spark.executorEnv.PYTHONPATH": "/opt/pipelines",
        },
        env_vars={
            "PYTHONPATH": "/opt/airflow",
            "NEO4J_HOST": "neo4j",
            "NEO4J_URL": "bolt://neo4j:7687",
        },
        application_args=[
            "--target-date",
            "{{ dag_run.conf.get('target_date') or '2026-11-09' }}",
        ],
        packages="com.clickhouse:clickhouse-jdbc:0.4.6,org.apache.httpcomponents.client5:httpclient5:5.2.1,org.neo4j:neo4j-connector-apache-spark_2.12:5.3.0_for_spark_3",
        driver_memory="1g",
        executor_memory="1g",
        executor_cores=2,
        total_executor_cores=2,
        verbose=True,
        execution_timeout=timedelta(minutes=30),
    )

    wait_for_logistics_logs >> compile_dbt_scan_events_query >> run_spark_neo4j_graph_refresh
