from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.models import DagRun

default_args = {
    'owner': 'senior_data_architecture_team',
    'depends_on_past': False,           
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,                       
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2)
}

with DAG(
    dag_id='frustration_experience_risks',
    default_args=default_args,
    schedule_interval=None,      
    start_date=datetime(2026, 11, 9),
    catchup=False,                       
    max_active_runs=1,                
    tags=['production', 'feature_store', 'spark', 'dbt', 'postgres', 'clickhouse', 'redis'],
) as dag:

    def get_most_recent_dag_run(dt, **kwargs):
        dag_runs = DagRun.find(dag_id="clickstream_collector_pipeline")
        dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
        return dag_runs[0].execution_date if dag_runs else dt

    wait_for_clickstream_logs = ExternalTaskSensor(
        task_id='wait_for_clickstream_collector_dump',
        external_dag_id='clickstream_collector_pipeline',
        external_task_id='flush_kafka_microbatches_to_clickhouse',
        execution_date_fn=get_most_recent_dag_run,
        allowed_states=['success'],
        failed_states=["failed", "skipped"],
        poke_interval=60,
        timeout=1800,
        mode='reschedule' 
    )

    compile_dbt_features = BashOperator(
        task_id="compile_dbt_pushdown_queries",
        cwd="/opt/airflow/data_pipelines/dbt",
        bash_command=(
            "dbt compile --profiles-dir . --vars '{\"target_date\": \"{{ data_interval_start.strftime(\"%Y-%m-%d\") }}\"}'"
        ),
    )

    run_spark_ux_feature_pipeline = SparkSubmitOperator(
        task_id="run_distributed_spark_ml_inference",
        application="/opt/airflow/data_pipelines/jobs/frustration_experience_risks/run.py",
        conn_id="spark_default",
        conf={
            "spark.driver.host": "airflow",
            "spark.master": "spark://spark-master:7077",
            "spark.executorEnv.PYTHONPATH": "/opt/pipelines",
        },
        env_vars={
            "PYTHONPATH": "/opt/airflow",
        },
        application_args=["--target-date", "{{ dag_run.conf.get('target_date', ds) }}"],
        packages="com.clickhouse:clickhouse-jdbc:0.6.0,org.neo4j:neo4j-connector-apache-spark_2.12:5.3.0_for_spark_3,org.postgresql:postgresql:42.7.2",
        driver_memory="512m",
        executor_memory="1g",
        executor_cores=2,
        verbose=True,
        execution_timeout=timedelta(minutes=45),
    )


    wait_for_clickstream_logs >> compile_dbt_features >> run_spark_ux_feature_pipeline
