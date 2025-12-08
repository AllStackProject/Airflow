# 📺 Video Transcode HLS Pipeline (Airflow + KubernetesExecutor)

이 DAG은 **원본 MP4 영상 다운로드 → 해상도별 트랜스코딩 → HLS 패키징 → S3 업로드 → 로컬 클린업** 전체 파이프라인을 KubernetesExecutor 기반으로 수행합니다.  
또한 **Slack 실패 알림 및 백엔드 API 상태 전송** 기능을 제공합니다.

---

## 🧰 기술 스택

| 항목 | 사용 기술 |
|------|-----------|
| Workflow Engine | Apache Airflow |
| Executor | KubernetesExecutor |
| Storage | AWS S3 (`privideo-original`, `privideo-output`) |
| Video Processing | ffmpeg |
| Node Storage | PVC (`pvc-hdd-airflow-worker-temp`) |
| Alerts | Slack Webhook |
| Backend Sync | Privideo Cloud API |


## 📦 주요 기능

### 1) 원본 영상 다운로드
- S3(`privideo-original`)에서 MP4 다운로드  
- 로컬 경로: `/workspace/{video_uuid}` (PVC 사용)

### 2) 병렬 트랜스코딩 (360p, 540p, 720p)
- 각 해상도별 Pod가 생성됨
- 해상도에 따라 CPU/MEM 리소스 자동 다르게 설정됨
- ffmpeg로 스케일링 후 MP4 저장 및 업로드

### 3) HLS 패키징
- 각 해상도별 `.m3u8 + .ts` 생성  
- master playlist(`video.m3u8`) 생성
- 결과를 S3(`privideo-output`)에 업로드

### 4) 정리 및 API Success 전송
- PVC 내 작업 폴더 삭제
- Privideo 백엔드에 `"SUCCESS"` 전송

## 🔔 Slack & Backend Fail 콜백

DAG 실패 시 다음이 수행됩니다.

1. Slack Webhook으로 메시지 전송
   <img width="753" height="365" alt="image" src="https://github.com/user-attachments/assets/35e206f7-7648-45b6-b326-369e3d7b4447" />
   - 실패 task 정보
   - DAG Run 링크
3. Privideo 백엔드에 상태 업데이트 (FAILED)

---

## ⚙️ KubernetesExecutor 동작 방식

### PodOverride 사용
각 Task는 다음 요소를 가진 Pod로 실행됩니다.

- `nodeSelector` — 특정 노드(`ip-10-0-0-12`)로 스케줄링
- `tolerations` — worker 노드 태인트 허용
- `PVC Mount` — `/workspace` 경로에 PVC 마운트
- 환경변수는 AWS Secret (`airflow-aws`)에서 주입

### 해상도별 리소스 예시

```python
if res == "360":
    return make_container("1000m", "1000m", "1Gi", "2Gi")
elif res == "540":
    return make_container("1000m", "2000m", "1Gi", "3Gi")
elif res == "720":
    return make_container("1000m", "3000m", "1Gi", "4Gi")
```

---

## 🧪 DAG 실행 방법

1. Airflow Web UI → Trigger DAG
JSON Payload 예시:
{
  "org_id": "1",
  "video_uuid": "abc123"
}
