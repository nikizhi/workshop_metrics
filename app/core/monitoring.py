from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import Counter


jobs_created_total = Counter(
    name="jobs_created_total",
    documentation="Общее количество задач, созданных через API",
    labelnames=["job_type", "queue"]
)

jobs_sent_to_queue_total = Counter(
    name="jobs_sent_to_queue_total",
    documentation="Общее количество задач, отправленных в очередь",
    labelnames=["job_type", "queue"]
)

def setup_monitoring(app: FastAPI):
    Instrumentator().instrument(app).expose(
        app=app,
        endpoint="/metrics",
        include_in_schema=False
    )