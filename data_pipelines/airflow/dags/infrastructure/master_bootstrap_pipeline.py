from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import DagModel
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.session import provide_session
from datetime import datetime, timedelta, timezone


logger = logging.getLogger("rtc_master_bootstrap")

CHILD_DAGS = [
    "clickstream_collector_pipeline",
    "wms_collector_pipeline",
    "logistics_graph_refresh_pipeline",
    "stochastic_logistics_tracking_pipeline",
    "frustration_experience_risks",
    "comprehensive_churn_decision_ensemble",
]


@provide_session
def _unpause_all_child_dags(session=None) -> None:
    dags = session.query(DagModel).filter(DagModel.dag_id.in_(CHILD_DAGS)).all()
    for dag_model in dags:
        if dag_model.is_paused:
            dag_model.is_paused = False
            dag_model.next_dagrun_create_after = datetime(2099, 1, 1, tzinfo=timezone.utc)
            logger.info(f"[MASTER_BOOTSTRAP] DAG '{dag_model.dag_id}' снят с паузы (плановый запуск подавлен).")
    session.commit()


@provide_session
def _pause_all_child_dags(session=None) -> None:
    dags = session.query(DagModel).filter(DagModel.dag_id.in_(CHILD_DAGS)).all()
    for dag_model in dags:
        dag_model.is_paused = True
        logger.info(f"[MASTER_BOOTSTRAP] DAG '{dag_model.dag_id}' возвращён на паузу.")
    session.commit()


default_args = {
    "owner": "realtime-commerce-data-eng",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=15),
}

with DAG(
    dag_id="master_bootstrap_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 11, 9),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["orchestration", "bootstrap", "end-to-end"],
) as dag:

    t0_unpause = PythonOperator(
        task_id="unpause_child_dags",
        python_callable=_unpause_all_child_dags,
    )

    t1_clickstream = TriggerDagRunOperator(
        task_id="run_clickstream_collector",
        trigger_dag_id="clickstream_collector_pipeline",
        execution_date=datetime(2026, 11, 9, tzinfo=timezone.utc),
        wait_for_completion=True,
        poke_interval=5,
        failed_states=["failed"],
    )

    t1_wms = TriggerDagRunOperator(
        task_id="run_wms_collector",
        trigger_dag_id="wms_collector_pipeline",
        execution_date=datetime(2026, 11, 9, tzinfo=timezone.utc),
        wait_for_completion=True,
        poke_interval=5,
        failed_states=["failed"],
    )

    t2_graph_refresh = TriggerDagRunOperator(
        task_id="run_logistics_graph_refresh",
        trigger_dag_id="logistics_graph_refresh_pipeline",
        execution_date=datetime(2026, 11, 9, tzinfo=timezone.utc),
        wait_for_completion=True,
        poke_interval=5,
        failed_states=["failed"],
    )

    t3_logistics_tracking = TriggerDagRunOperator(
        task_id="run_stochastic_logistics_tracking",
        trigger_dag_id="stochastic_logistics_tracking_pipeline",
        execution_date=datetime(2026, 11, 9, tzinfo=timezone.utc),
        wait_for_completion=True,
        poke_interval=5,
        failed_states=["failed"],
    )

    t3_frustration_risks = TriggerDagRunOperator(
        task_id="run_frustration_experience_risks",
        trigger_dag_id="frustration_experience_risks",
        execution_date=datetime(2026, 11, 9, tzinfo=timezone.utc),
        wait_for_completion=True,
        poke_interval=5,
        failed_states=["failed"],
    )

    t4_redis_warmup = TriggerDagRunOperator(
        task_id="run_churn_ensemble_and_redis_warmup",
        trigger_dag_id="comprehensive_churn_decision_ensemble",
        execution_date=datetime(2026, 11, 9, tzinfo=timezone.utc),
        wait_for_completion=True,
        poke_interval=5,
        failed_states=["failed"],
    )

    t5_pause = PythonOperator(
        task_id="pause_child_dags",
        python_callable=_pause_all_child_dags,
        trigger_rule="all_done",
    )

    (
        t0_unpause
        >> [t1_clickstream, t1_wms]
        >> t2_graph_refresh
        >> [t3_logistics_tracking, t3_frustration_risks]
        >> t4_redis_warmup
        >> t5_pause
    )
