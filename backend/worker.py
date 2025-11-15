# app/worker.py
import os
from celery import Celery
from .utils.config import get_config
from .utils.setlogger import setup_logger
config = get_config()
logger = setup_logger(f"{__name__}", level=config.LOG_LEVEL)

# Fetch Celery configuration from environment variables
celery_app = Celery(
    'background_tasks',
    broker=os.getenv("CELERY_BROKER_URL", config.REDIS_BROKER_URL),
    backend=os.getenv("CELERY_RESULT_BACKEND", config.REDIS_BACKEND_URL)
    )

celery_app.conf.update(result_expires=3600,   # Results expire after 1 hour
                       worker_prefetch_multiplier=1,  # 과도한 프리페칭 방지
                       task_acks_late=True,  # 재시도 시 메시지 손실 방지
                       task_reject_on_worker_lost=True)  # 워커 다운 시 재시도)  
celery_app.autodiscover_tasks(['app.tasks'])
