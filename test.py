from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from datetime import datetime


# --------------------------
# 콜백 함수
# --------------------------
def test_success_callback(context):
    print("🎉 SUCCESS CALLBACK 실행됨!")
    print("context keys:", context.keys())

    dag_run = context["dag_run"]
    conf = dag_run.conf

    print("dag_run.conf:", conf)
    print("task_id:", context["task_instance"].task_id)


def test_failure_callback(context):
    print("🔥 FAILURE CALLBACK 실행됨!")
    print("context keys:", context.keys())

    dag_run = context["dag_run"]
    conf = dag_run.conf

    print("dag_run.conf:", conf)
    print("task_id:", context["task_instance"].task_id)


# --------------------------
# Task
# --------------------------
@task
def success_task():
    print("task 성공 실행")


@task
def fail_task():
    print("task 실패 실행 예정")
    raise ValueError("일부러 예외 발생!")


# --------------------------
# DAG
# --------------------------
with DAG(
    dag_id="callback_test_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    on_success_callback=test_success_callback,
    on_failure_callback=test_failure_callback,
) as dag:

    ok = success_task()
    ng = fail_task()

    ok >> ng
