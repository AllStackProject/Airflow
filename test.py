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

    text = f"""
🔥 *Task Failed!*

*Task Info*
- Task ID: `{ti.task_id}`
- DAG ID: `{ti.dag_id}`
- Run ID: `{ti.run_id}`
- Try Number: {ti.try_number}
- Hostname: {ti.hostname}
- State: {ti.state}

*Config*
- video_uuid: `{video_uuid}`
- org_id: `{org_id}`

*Exception*
```python
{exception}
