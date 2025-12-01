from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from datetime import datetime


# --------------------------
# 콜백 함수
# --------------------------
def test_success_callback(context):
    print("🎉 SUCCESS CALLBACK 실행됨!")


# --------------------------
# Task
# --------------------------
@task
def success_task():
    print("task 성공 실행")


# --------------------------
# DAG
# --------------------------
with DAG(
    dag_id="callback_test_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    on_success_callback=test_success_callback
) as dag:

    ok = success_task()

    ok
