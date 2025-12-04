from airflow import DAG
from airflow.decorators import task
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

    ti = context["task_instance"]
    dag_conf = context["dag_run"].conf

    org_id = dag_conf.get("org_id")
    video_uuid = dag_conf.get("video_uuid")
    exception = context.get("exception")

    # ---------------------------------------------------
    # 🔥 가장 안정적인 문자열 생성 방식: 줄 배열 + join
    # f-string 깨짐 / 공백 문제 / ``` 블록 문제 없음
    # ---------------------------------------------------
    lines = [
        "🔥 *Task Failed!*",
        "",
        "*Task Info*",
        f"- Task ID: `{ti.task_id}`",
        f"- DAG ID: `{ti.dag_id}`",
        f"- Run ID: `{ti.run_id}`",
        f"- Try Number: {ti.try_number}",
        f"- Hostname: {ti.hostname}",
        f"- State: {ti.state}",
        "",
        "*Config*",
        f"- video_uuid: `{video_uuid}`",
        f"- org_id: `{org_id}`",
        "",
        "*Exception*",
        "```python",
        str(exception),
        "```",
    ]

    text = "\n".join(lines)

    # Slack URL
    slack_url = f"https://hooks.slack.com/services/{Variable.get('slack_url')}"

    payload = {
        "user_name": "airflow",
        "text": text,
        "icon_emoji": ":x:"
    }

    headers = {"content-type": "application/json"}

    # Slack 전송
    requests.post(slack_url, json=payload, headers=headers)


# --------------------------
# Task
# --------------------------
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
