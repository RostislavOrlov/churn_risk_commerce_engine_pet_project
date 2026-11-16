from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import DagRun
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys

def _run_redis_sync(**kwargs):
    if "/opt/airflow" not in sys.path:
        sys.path.insert(0, "/opt/airflow")
    from data_pipelines.jobs.ml_inference_orchestrator.redis_sync import sync_marts_to_redis
    sync_marts_to_redis(**kwargs)


def get_latest_successful_run(target_dag_id: str):
    def _fetch(dt, **kwargs):
        dag_runs = DagRun.find(dag_id=target_dag_id, state="success")
        if not dag_runs:
            return dt
        dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
        return dag_runs[0].execution_date
    return _fetch


default_args = {
    "owner": "senior_core_mlops_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(hours=1.5),
}

with DAG(
    dag_id="comprehensive_churn_decision_ensemble",
    default_args=default_args,
    schedule_interval=None,  
    start_date=datetime(2026, 11, 9),
    catchup=False,   
    max_active_runs=1,
    template_searchpath=["/opt/airflow"],
    tags=["production", "ml_ops", "ensemble", "spark", "postgres", "timescaledb", "highload"],
) as dag:

    wait_for_ux_features = ExternalTaskSensor(
        task_id="wait_for_ux_features_buffer_fill",
        external_dag_id="frustration_experience_risks",
        external_task_id="run_distributed_spark_ml_inference",
        execution_date_fn=get_latest_successful_run("frustration_experience_risks"),
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        poke_interval=15,
        timeout=1800,
        mode="reschedule",
    )

    wait_for_logistics_features = ExternalTaskSensor(
        task_id="wait_for_logistics_features_buffer_fill",
        external_dag_id="stochastic_logistics_tracking_pipeline",
        external_task_id="run_distributed_spark_logistics_tracking_job",
        execution_date_fn=get_latest_successful_run("stochastic_logistics_tracking_pipeline"),
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        poke_interval=15,
        timeout=1800,
        mode="reschedule",
    )

    build_logistics_features_buffer = PostgresOperator(
        task_id="execute_logistics_features_buffer_consolidation",
        postgres_conn_id="postgres_analytics_dw",
        sql="data_pipelines/database/sql_marts/logistics_features_buffer_build.sql",
        autocommit=True,
    )

    clear_all_staging_buffers = PostgresOperator(
        task_id="execute_monolithic_staging_buffers_truncation",
        postgres_conn_id="postgres_analytics_dw",
        sql="""
            DO $$
            DECLARE
                tbl TEXT;
                tables TEXT[] := ARRAY[
                    'stg_analytics.stg_ml_user_churn_scores',
                    'stg_analytics.stg_incident_log',
                    'stg_analytics.stg_preemptive_compensations',
                    'stg_analytics.stg_node_dynamic_capacities'
                ];
            BEGIN
                FOREACH tbl IN ARRAY tables LOOP
                    IF to_regclass(tbl) IS NOT NULL THEN
                        EXECUTE format('TRUNCATE TABLE %s;', tbl);
                    END IF;
                END LOOP;
            END $$;
        """,
        autocommit=True,
    )

    run_central_ml_inference = SparkSubmitOperator(
        task_id="execute_central_ml_inference_orchestrator",
        application="/opt/airflow/data_pipelines/jobs/ml_inference_orchestrator/run.py",
        conn_id="spark_default",
        conf={
            "spark.driver.host": "airflow",
            "spark.executorEnv.PYTHONPATH": "/opt/pipelines",
        },
        env_vars={
            "PYTHONPATH": "/opt/airflow",
        },
        application_args=[
            "--target-date",
            "{{ dag_run.conf.get('target_date') or params.get('target_date') or '2026-11-09' }}",
        ],
        packages="org.postgresql:postgresql:42.7.2",
        driver_memory="512m",
        executor_memory="1g",
        executor_cores=2,
        total_executor_cores=2,
        verbose=True,
        execution_timeout=timedelta(minutes=30),
    )

    build_complex_analytics_cube = PostgresOperator(
        task_id="execute_postgres_complex_cube_calculation",
        postgres_conn_id="postgres_analytics_dw",
        sql="data_pipelines/database/sql_marts/frustration_experience_risks/complex_mart_build.sql",
        autocommit=True,
    )

    sync_marts_to_online_redis = PythonOperator(
        task_id="sync_dw_marts_to_online_redis",
        python_callable=_run_redis_sync,
    )

    emit_ensemble_prometheus_metrics = BashOperator(
        task_id="emit_ensemble_prometheus_metrics",
        bash_command=(
            'cat <<EOF | curl -sS -m 5 --data-binary @- http://pushgateway:9091/metrics/job/comprehensive_churn_decision_ensemble || '
            'echo "Pushgateway not deployed in local compose, skipping metric push."\n'
            '# TYPE churn_ensemble_pipeline_success_heartbeat gauge\n'
            'churn_ensemble_pipeline_success_heartbeat 1\n'
            '# TYPE churn_ensemble_last_success_timestamp_seconds gauge\n'
            'churn_ensemble_last_success_timestamp_seconds $(date +%s)\n'
            'EOF'
        ),
    )


    [wait_for_ux_features, wait_for_logistics_features] >> build_logistics_features_buffer
    build_logistics_features_buffer >> clear_all_staging_buffers >> run_central_ml_inference 
    run_central_ml_inference >> build_complex_analytics_cube >> sync_marts_to_online_redis 
    sync_marts_to_online_redis >> emit_ensemble_prometheus_metrics
