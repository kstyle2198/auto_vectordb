# backend/core/celery_app.py
import os
import json
from celery import Celery
from celery.signals import worker_process_init, worker_ready
from pydantic import ValidationError
from core.dependencies import init_embedding_model  

from utils.config import get_config
from utils.setlogger import setup_logger
config = get_config()
logger = setup_logger(f"{__name__}", level=config.LOG_LEVEL)

# Fetch Celery configuration from environment variables
celery_app = Celery(
    'worker',
    broker=os.getenv("CELERY_BROKER_URL", config.REDIS_BROKER_URL),
    backend=os.getenv("CELERY_RESULT_BACKEND", config.REDIS_BACKEND_URL),
    include=["routers.task_router"]
    )

celery_app.conf.update(result_expires=3600,   # Results expire after 1 hour
                       worker_prefetch_multiplier=1,  # 과도한 프리페칭 방지
                       task_acks_late=True,  # 재시도 시 메시지 손실 방지
                       task_reject_on_worker_lost=True)  # 워커 다운 시 재시도)  

@worker_process_init.connect
def init_worker(**kwargs):
    """
    각 worker process 시작 시 1회 실행
    """
    logger.info("Initializing worker process...")

    try:
        init_embedding_model()  # ⭐ 핵심
        logger.info("Embedding model initialized successfully")
    except Exception as e:
        logger.error(f"Embedding model init failed: {e}")
        raise