from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import boto3
import os
import subprocess
import shutil
import requests
from kubernetes.client import models as k8s
from airflow.operators.python import get_current_context


# -------------------------------
# 기본 설정
# -------------------------------
BUCKET_ORIGINAL = "privideo-original"
BUCKET_OUTPUT = "privideo-output"
OUTPUT_DIR = "/workspace"
RESOLUTIONS = ["360", "540", "720"]


# -------------------------------
# 리소스 컨테이너 생성
# -------------------------------
def make_container(cpu_req, cpu_limit, mem_req, mem_limit):
    return k8s.V1Container(
        name="base",
        image="leeyonghun/airflow-ffmpeg:v4",
        env_from=[
            k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name="airflow-aws"))
        ],
        resources=k8s.V1ResourceRequirements(
            requests={"cpu": cpu_req, "memory": mem_req},
            limits={"cpu": cpu_limit, "memory": mem_limit},
        ),
    )


# 해상도별 리소스 매핑
def get_transcode_container(res):
    if res == "360":
        return make_container("1000m", "1000m", "1Gi", "2Gi")
    elif res == "540":
        return make_container("1000m", "2000m", "1Gi", "3Gi")
    elif res == "720":
        return make_container("1000m", "3000m", "1Gi", "4Gi")
    else:
        raise ValueError("Unsupported resolution")


# 패키징 컨테이너 (저사양)
package_container = make_container("500m", "1000m", "1Gi", "2Gi")


# -------------------------------
# PVC 마운트 Pod Override
# -------------------------------
def exec_config(container):
    return {
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                tolerations=[
                    k8s.V1Toleration(
                        key="role",
                        operator="Equal",
                        value="airflow-worker",
                        effect="NoSchedule"
                    )
                ],
                node_selector={
                    "kubernetes.io/hostname": "ip-10-0-0-12"
                },
                containers=[
                    k8s.V1Container(
                        name=container.name,
                        image=container.image,
                        env_from=container.env_from,
                        resources=container.resources,
                        volume_mounts=[
                            k8s.V1VolumeMount(
                                name="worker-temp",
                                mount_path="/workspace"
                            )
                        ],
                    )
                ],
                volumes=[
                    k8s.V1Volume(
                        name="worker-temp",
                        persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                            claim_name="pvc-hdd-airflow-worker-temp"
                        )
                    )
                ],
                restart_policy="Never",
            )
        )
    }


# -------------------------------
# API 콜백 함수
# -------------------------------
def dag_fail_callback(context):
    print("============ FAILED ============")
    
    org_id = context["dag_run"].conf.get("org_id")
    video_uuid = context["dag_run"].conf.get("video_uuid")

    url = f"https://www.privideo.cloud/api/{org_id}/video/airflow/status"
    payload = {
        "video_uuid": video_uuid,
        "status": "FAILED",
        "message": f"[DAG Failed] Failed"
    }

    requests.post(url, json=payload)

# -------------------------------
# 1) 다운로드
# -------------------------------
@task(executor_config=exec_config(package_container))
def download_video():

    context = get_current_context()
    dag_run = context["dag_run"]

    org_id = dag_run.conf.get("org_id")
    video_uuid = dag_run.conf.get("video_uuid")

    s3 = boto3.client("s3")

    local_dir = f"{OUTPUT_DIR}/{video_uuid}"
    os.makedirs(local_dir, exist_ok=True)

    s3_key = f"hls/org-{org_id}/{video_uuid}/video.mp4"
    local_input = f"{local_dir}/video.mp4"

    print(f"⬇️ Download: s3://{BUCKET_ORIGINAL}/{s3_key}")
    s3.download_file(BUCKET_ORIGINAL, s3_key, local_input)

    return local_dir


# -------------------------------
# 2) 트랜스코딩
# -------------------------------
def build_transcode_task(resolution):

    container = get_transcode_container(resolution)

    @task(
        task_id=f"transcode_video_{resolution}p",
        executor_config=exec_config(container)
    )
    def _transcode(local_dir: str):

        context = get_current_context()
        dag_run = context["dag_run"]

        org_id = dag_run.conf.get("org_id")
        video_uuid = dag_run.conf.get("video_uuid")

        input_local = f"{local_dir}/video.mp4"
        output_local = f"{local_dir}/video_{resolution}p.mp4"

        cmd = (
            f"ffmpeg -y -i {input_local} "
            f"-vf scale=-2:{resolution} "
            f"-c:v libx264 -preset veryfast -c:a aac {output_local}"
        )

        print(f"🎬 Transcoding {resolution}p")
        subprocess.run(cmd, shell=True, check=True)

        s3 = boto3.client("s3")
        key = f"hls/org-{org_id}/{video_uuid}/video_{resolution}p.mp4"
        s3.upload_file(output_local, BUCKET_ORIGINAL, key)

        return output_local

    return _transcode


# -------------------------------
# 3) 패키징 + 업로드
# -------------------------------
@task(executor_config=exec_config(package_container))
def packaging_and_upload(trans_outputs: list):

    context = get_current_context()
    dag_run = context["dag_run"]

    org_id = dag_run.conf.get("org_id")
    video_uuid = dag_run.conf.get("video_uuid")

    s3 = boto3.client("s3")

    local_dir = f"{OUTPUT_DIR}/{video_uuid}"
    hls_dir = f"{local_dir}/hls"
    os.makedirs(hls_dir, exist_ok=True)

    # 각 해상도 mp4 → m3u8 + ts 변환
    for mp4_path in trans_outputs:
        filename = os.path.basename(mp4_path)
        base_name = filename.replace(".mp4", "")

        m3u8_path = f"{hls_dir}/{base_name}.m3u8"
        seg_dir = f"{hls_dir}/{base_name}"
        os.makedirs(seg_dir, exist_ok=True)

        seg_pattern = f"{seg_dir}/{base_name}_%05d.ts"

        cmd = (
            f"ffmpeg -y -i {mp4_path} -c copy "
            f"-map 0 -f hls -hls_time 10 -hls_playlist_type vod "
            f"-hls_segment_filename '{seg_pattern}' "
            f"{m3u8_path}"
        )

        print(f"[HLS] Packaging {base_name}")
        subprocess.run(cmd, shell=True, check=True)

    # Master Playlist 생성
    master_path = f"{hls_dir}/video.m3u8"
    with open(master_path, "w") as f:
        f.write("#EXTM3U\n")
        f.write("#EXT-X-VERSION:3\n\n")

        renditions = {
            "360": ("800000", "640x360"),
            "540": ("1400000", "960x540"),
            "720": ("2800000", "1280x720")
        }

        for res in ["360", "540", "720"]:
            bandwidth, resolution = renditions[res]
            f.write(f"#EXT-X-STREAM-INF:BANDWIDTH={bandwidth},RESOLUTION={resolution}\n")
            f.write(f"video_{res}p.m3u8\n\n")

    print("📝 Master Playlist 완료:", master_path)

    # 업로드
    for root, dirs, files in os.walk(hls_dir):
        for file in files:
            local_path = os.path.join(root, file)
            key = f"hls/org-{org_id}/{video_uuid}/{local_path.replace(hls_dir, '').lstrip('/')}"
            print("⬆️ Upload:", key)
            s3.upload_file(local_path, BUCKET_OUTPUT, key)

    return True


# -------------------------------
# 4) 정리
# -------------------------------
@task(executor_config=exec_config(package_container))
def cleanup_local_files():

    context = get_current_context()
    dag_run = context["dag_run"]

    org_id = dag_run.conf.get("org_id")
    video_uuid = dag_run.conf.get("video_uuid")

    target = f"{OUTPUT_DIR}/{video_uuid}"

    print("🧹 Cleaning up:", target)
    if os.path.exists(target):
        shutil.rmtree(target, ignore_errors=True)

    # 백엔드로 성공 메세지 전송
    print("============ SUCCESS ============")
    
    url = f"https://www.privideo.cloud/api/{org_id}/video/airflow/status"
    payload = {
        "video_uuid": video_uuid,
        "status": "SUCCESS",
        "message": "[DAG Success] Success upload"
    }

    requests.post(url, json=payload)

    print("🧼 PVC cleanup complete.")
    return True


# -------------------------------
# DAG
# -------------------------------
with DAG(
    dag_id="video_transcode_hls_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args = {
        on_failure_callback=dag_fail_callback,
    }
) as dag:

    local_dir = download_video()

    trans_tasks = [
        build_transcode_task(r)(local_dir)
        for r in RESOLUTIONS
    ]

    upload = packaging_and_upload(trans_tasks)

    clean = cleanup_local_files()

    local_dir >> trans_tasks >> upload >> clean
