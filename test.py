from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from datetime import datetime

from functools import partial


# --------------------------
# 콜백 함수
# --------------------------
def test_success_callback(context):
    print("🎉 SUCCESS CALLBACK 실행됨!")
    print("dag_run.conf =", context["dag_run"].conf)

def on_failure_callback(context):
    print("🔥 FAILURE CALLBACK 실행됨!")
    
    org_id = context["dag_run"].conf.get("org_id")
    video_uuid = context["dag_run"].conf.get("video_uuid")

	# Task 인스턴스를 가져와 어떤 Task에서 에러가 났는지 확인할 수 있도록 한다.
	text = str(context['task_instance'])   
    
    # exception 정보가 있으면 가져온다.
    text += f"``` {str(context.get('exception'))} ```"

    text += f"video_uuid = {video_uuid}"
    text += f"org_id = {org_id}"

    url = f"https://hooks.slack.com/services/{Variable.get('slack_url')}"

    headers = {
        'content-type': 'application/json',
    }

    payload = { "user_name": "airflow", "text": message, "icon_emoji": ":cry:" }

    requests.post(url, json=payload, headers=headers)


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
    default_args = {
        "on_success_callback": test_success_callback,
        "on_failure_callback": test_failure_callback,
    }
) as dag:

    success_task()
