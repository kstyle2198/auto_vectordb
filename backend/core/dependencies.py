# core/dependencies.py

from core.state import app_state
from contextlib import contextmanager

from utils.config import get_config
config = get_config()


from FlagEmbedding import BGEM3FlagModel
from threading import Lock

_embedding_model = None
_lock = Lock()

def init_embedding_model(model_name=config.EMBED_MODEL_PATH):
    global _embedding_model
    with _lock:
        if _embedding_model is None:
            _embedding_model = BGEM3FlagModel(model_name, use_fp16=True)

def get_embedding_model():
    global _embedding_model
    if _embedding_model is None:
        init_embedding_model()
    return _embedding_model


def get_es():
    return app_state.es


def get_pg_pool():
    return app_state.pg_pool


def get_pg_connection():
    pool = get_pg_pool()
    return pool.getconn()


def release_pg_connection(conn):
    if conn:
        app_state.pg_pool.putconn(conn)


@contextmanager
def pg_connection():
    conn = get_pg_connection()
    try:
        yield conn
    finally:
        release_pg_connection(conn)