from elasticsearch import Elasticsearch
from psycopg2.pool import SimpleConnectionPool
from FlagEmbedding import BGEM3FlagModel


class AppState:
    pg_pool: SimpleConnectionPool = None
    es: Elasticsearch = None
    embedding_model: BGEM3FlagModel = None


app_state = AppState()