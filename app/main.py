from fastapi import FastAPI

from app.core.settings import settings
from app.core.logging_config import setup_logging
from app.routes.job import router as jobs_router
from app.middleware.request_id import request_id_middleware
from app.core.monitoring import setup_monitoring


setup_logging()

app = FastAPI(title=settings.APP_TITLE)
app.middleware("http")(request_id_middleware)

app.include_router(jobs_router)

setup_monitoring(app)