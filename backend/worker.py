# backend/worker.py
import os
import json
from celery import Celery
from pydantic import ValidationError
from utils.config import get_config
from utils.setlogger import setup_logger
config = get_config()
logger = setup_logger(f"{__name__}", level=config.LOG_LEVEL)

# Fetch Celery configuration from environment variables
celery_app = Celery(
    'worker',
    broker=os.getenv("CELERY_BROKER_URL", config.REDIS_BROKER_URL),
    backend=os.getenv("CELERY_RESULT_BACKEND", config.REDIS_BACKEND_URL)
    )

celery_app.conf.update(result_expires=3600,   # Results expire after 1 hour
                       worker_prefetch_multiplier=1,  # 과도한 프리페칭 방지
                       task_acks_late=True,  # 재시도 시 메시지 손실 방지
                       task_reject_on_worker_lost=True)  # 워커 다운 시 재시도)  

from process.parsing import DoclingParser
parser = DoclingParser(output_base_path="./docs/parsed")

import redis 
redis_pubsub_client = redis.Redis.from_url(config.REDIS_PUBSUB_URL)
from collections import defaultdict

from langchain_core.documents import Document
def document_to_dict(doc: Document) -> dict:
    return {
        "page_content": doc.page_content,
        "metadata": doc.metadata
        }


@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def long_running_task(self, pdf_path: str):
    """
    작업 완료 후 Pub/Sub으로 결과를 발행하는 태스크
    """
    task_id = self.request.id
    channel_name = f"task_results:{task_id}"

    file_path, file_name = os.path.split(pdf_path)
    target_file_path = file_path.split("uploaded")[1].replace("\\", "/")
    target_file_path = target_file_path.split("/")

    # 파일 폴더 경로에서 level cat 추출 (최대 4개까지)
    cats = defaultdict(str)
    for i in range(1,5,1):
        try: cats[f"lv{i}_cat"] = target_file_path[i]
        except: cats[f"lv{i}_cat"] = ""
    cats = dict(cats)
    lv1_cat, lv2_cat, lv3_cat, lv4_cat = cats["lv1_cat"], cats["lv2_cat"], cats["lv3_cat"], cats["lv4_cat"]

    try:
        logger.info(f"Task {task_id} started...")
        docs = parser.parse_pdf_by_page(pdf_path=pdf_path, lv1_cat=lv1_cat, lv2_cat=lv2_cat, lv3_cat=lv3_cat, lv4_cat=lv4_cat)
        dict_result = [document_to_dict(doc) for doc in docs]
        logger.info(f"Task {task_id} finished.")
        
        # 성공 결과를 JSON으로 만들어 채널에 발행
        payload = json.dumps({"event": "task_result", "data": dict_result})
        redis_pubsub_client.publish(channel_name, payload)
        
        return dict_result  # Celery 백엔드에도 결과 저장
    
    except ConnectionError as exc:
        # 네트워크 에러는 재시도
        logger.debug("ConnectionError --> Retry")
        raise self.retry(exc=exc, countdown=30)
    except ValidationError as exc:
        # 검증 에러는 재시도 의미 없음
        logger.error(f"ValidationError : {exc}")
        return {"error": "Invalid email address"}
    except Exception as e:
        logger.error(f"Task {task_id} failed: {e}")
        # 실패 결과를 JSON으로 만들어 채널에 발행
        payload = json.dumps({"event": "task_error", "data": str(e)})
        redis_pubsub_client.publish(channel_name, payload)
        # Celery가 이 태스크를 '실패'로 기록하도록 예외를 다시 발생시킴
        raise

