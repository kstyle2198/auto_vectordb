import json
import redis  # 동기 redis 클라이언트 (Celery 작업자용)
from .utils.setlogger import setup_logger
from .utils.config import get_config
config = get_config()
logger = setup_logger(f"{__name__}", level=config.LOG_LEVEL)



