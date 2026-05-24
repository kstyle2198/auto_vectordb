# core/dependencies.py

from core.state import app_state
from contextlib import contextmanager


def get_embedding_model():
    return app_state.embedding_model


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