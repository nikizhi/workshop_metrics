import json
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request
from loguru import logger

from app.core.settings import settings
from app.core.redis import redis_client
from app.core.database import SessionDep
from app.core.monitoring import jobs_created_total, jobs_sent_to_queue_total
from app.models.job import JobOut, JobCreate, JobsSummaryOut, JobDB, JobType, QueueName
from app.repositories import job as job_repo
from app.tasks.celery_tasks import run_job_task

router = APIRouter(prefix="/jobs", tags=["Jobs"])

JOBS_SUMMARY_CACHE_KEY = "jobs:summary"


def _build_job_out(job: JobDB) -> JobOut:
    payload = json.loads(job.payload) if job.payload is not None else None
    result = json.loads(job.result) if job.result is not None else None

    return JobOut(
        id=job.id,
        title=job.title,
        job_type=JobType(job.job_type),
        status=job.status,
        created_at=job.created_at,
        finished_at=job.finished_at,
        error=job.error,
        payload=payload,
        result=result
    )


@router.post("", response_model=JobOut)
async def create_job(
    request: Request,
    payload: JobCreate,
    session: SessionDep,
    queue_name: QueueName = Query(default=QueueName.DEFAULT),
    priority: int = Query(default=5, ge=0, le=9)
) -> JobOut:
    request_id = request.state.request_id

    request_log = logger.bind(
        component="api",
        request_id=request_id
    )

    job = await job_repo.create_job(session, payload)

    jobs_created_total.labels(
        job_type=payload.job_type.value,
        queue=queue_name.value
    ).inc()

    job_log = request_log.bind(job_id=str(job.id))
    job_log.bind(
        event="job_created",
        job_type=payload.job_type.value,
    ).info("Job created")

    await redis_client.delete(JOBS_SUMMARY_CACHE_KEY)

    run_job_task.apply_async(
        args=[str(job.id)], 
        queue=queue_name.value,
        priority=priority
    )

    jobs_sent_to_queue_total.labels(
        job_type=payload.job_type.value,
        queue=queue_name.value
    ).inc()

    job_log.bind(
        event="task_sent_to_queue",
        queue=queue_name.value,
        priority=priority,
    ).info("Task sent to queue")

    return _build_job_out(job)


@router.get("/summary", response_model=JobsSummaryOut)
async def get_jobs_summary(
    request: Request,
    session: SessionDep
) -> JobsSummaryOut:
    request_id = request.state.request_id

    request_log = logger.bind(
        component="api",
        request_id=request_id,
    )

    cached_summary = await redis_client.get(JOBS_SUMMARY_CACHE_KEY)

    if cached_summary is not None:
        request_log.bind(
            event="summary_cache_hit",
        ).info("Jobs summary returned from Redis")
        cached_data = json.loads(cached_summary)
        return JobsSummaryOut(**cached_data)
    
    request_log.bind(
        event="summary_cache_miss",
    ).info("Jobs summary returned from PostgreSQL")
    
    summary = await job_repo.get_jobs_summary(session=session)
    summary_json = json.dumps(summary.model_dump())
    await redis_client.set(JOBS_SUMMARY_CACHE_KEY, summary_json)
    await redis_client.expire(JOBS_SUMMARY_CACHE_KEY, settings.REDIS_CACHE_TTL)

    return summary


@router.get("/{job_id}", response_model=JobOut)
async def get_job(
    request: Request,
    job_id: UUID,
    session: SessionDep,
) -> JobOut:
    request_id = request.state.request_id

    request_log = logger.bind(
        component="api",
        request_id=request_id,
    )

    job = await job_repo.get_job(session, job_id)
    if job is None:
        request_log.bind(
            event="job_not_found",
            job_id=str(job_id),
        ).warning("Job not found")
        
        raise HTTPException(status_code=404, detail="Job not found")

    return _build_job_out(job)