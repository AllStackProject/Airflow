from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from datetime import datetime

from functools import partial


# --------------------------
# 콜백 함수
# --------------------------
def test_success_callback(context, video_uuid=None):
    print("🎉 SUCCESS CALLBACK 실행됨!")
    print("partial 주입")
    print("video_uuid =", video_uuid)
    print("dag_run.conf =", context["dag_run"].conf)


def test_failure_callback(context):
    print("🔥 FAILURE CALLBACK 실행됨!")


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
    on_success_callback=partial(test_success_callback, video_uuid="abcd-1234"),
    on_failure_callback=test_failure_callback,
) as dag:

    success_task()
