from contextlib import asynccontextmanager

from elasticsearch import Elasticsearch
from psycopg2.pool import SimpleConnectionPool
from FlagEmbedding import BGEM3FlagModel

from core.state import app_state
from utils.config import get_config
from utils.setlogger import setup_logger
config = get_config()
logger = setup_logger(f"{__name__}", level=config.LOG_LEVEL)

# ---------------------------------------------------
# 설정
# ---------------------------------------------------
POSTGRES_CONFIG = {
    "host": config.POSTGRES_HOST,
    "dbname": config.POSTGRES_DB,
    "user": config.POSTGRES_USER,
    "password": config.POSTGRES_PW,
}

ELASTICSEARCH_URL = config.ES_URL

BGE_M3_MODEL_PATH = config.EMBED_MODEL_PATH  


# ---------------------------------------------------
# Lifespan
# ---------------------------------------------------
@asynccontextmanager
async def lifespan(app):

    logger.info("===== Startup =====")

    try:

        # PostgreSQL
        app_state.pg_pool = SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            **POSTGRES_CONFIG,
            )

        logger.info("PostgreSQL pool initialized")

        # Elasticsearch
        app_state.es = Elasticsearch(ELASTICSEARCH_URL,)

        logger.info("Elasticsearch initialized")

        # Embedding Model
        app_state.embedding_model = BGEM3FlagModel(BGE_M3_MODEL_PATH, use_fp16=False,)

        logger.info("Embedding model loaded")

        logger.info("===== Startup Complete =====")

        yield

    finally:

        logger.info("===== Shutdown =====")

        try:
            if app_state.pg_pool:
                app_state.pg_pool.closeall()

        except Exception as e:
            logger.error(f"PG pool close error: {e}")

        try:
            if app_state.es:
                app_state.es.close()

        except Exception as e:
            logger.error(f"ES close error: {e}")

        app_state.embedding_model = None
