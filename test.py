from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.models import Variable
from datetime import datetime
import requests


# --------------------------
# 콜백 함수 (성공)
# --------------------------
def test_success_callback(context):
    print("🎉 SUCCESS CALLBACK 실행됨!")
    print("dag_run.conf =", context["dag_run"].conf)


# --------------------------
# 콜백 함수 (실패)
# --------------------------
def test_failure_callback(context):
    print("🔥 FAILURE CALLBACK 실행됨!")

    dag_conf = context["dag_run"].conf
    org_id = dag_conf.get("org_id")
    video_uuid = dag_conf.get("video_uuid")

    # 실패한 Task 정보
    text = str(context['task_instance'])

    # 예외 정보
    exception = context.get('exception')
    if exception:
        text += f"\nException: ```{str(exception)}```"

    # 추가 정보
    text += f"\nvideo_uuid = {video_uuid}"
    text += f"\norg_id = {org_id}"

    # Slack URL
    slack_url = f"https://hooks.slack.com/services/{Variable.get('slack_url')}"

    payload = {
        "user_name": "airflow",
        "text": text,
        "icon_emoji": ":cry:"
    }

    headers = {"content-type": "application/json"}

    # Slack 전송
    requests.post(slack_url, json=payload, headers=headers)


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
    default_args={
        "on_success_callback": test_success_callback,
        "on_failure_callback": test_failure_callback,
    }
) as dag:
    fail_task()
