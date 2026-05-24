import json
import psycopg2

from elasticsearch import Elasticsearch, helpers

from utils.config import get_config
from utils.setlogger import setup_logger
config = get_config()
logger = setup_logger(f"{__name__}", level=config.LOG_LEVEL)


class ElasticsearchIndexer:

    def __init__(self, es_index_name: str, table_name: str):
        """
        PostgreSQL → Elasticsearch Hybrid Indexer
        """

        self.es_index_name = es_index_name
        self.table_name = table_name

        self.es = Elasticsearch("http://localhost:9200")

    # =========================================================
    # PostgreSQL
    # =========================================================

    def _get_pg_connection(self):

        return psycopg2.connect(
            host=config.POSTGRES_HOST,
            dbname=config.POSTGRES_DB,
            user=config.POSTGRES_USER,
            password=config.POSTGRES_PW
        )
    
    # =========================================================
    # Get All Index Names
    # =========================================================

    def get_all_index_names(self):
        """
        Elasticsearch 클러스터에 존재하는
        모든 인덱스 이름 조회
        """

        try:
            indices_dict = self.es.indices.get_alias(index="*")
            index_names = list(indices_dict.keys())
            logger.info(f"Retrieved {len(index_names)} indices from Elasticsearch.")
            return index_names

        except Exception as e:
            logger.error(f"Error fetching all index names: {e}")
            return []

    # =========================================================
    # Elasticsearch Index
    # =========================================================

    def create_index(self):
        """
        Hybrid Search용 Elasticsearch Index 생성
        """

        mapping = {
            "settings": {
                "index": {
                    "knn": True
                }
            },
            "mappings": {
                "properties": {

                    "id": {
                        "type": "keyword"
                    },

                    "hashed_content": {
                        "type": "keyword"
                    },

                    "page_content": {
                        "type": "text"
                    },

                    "metadata": {
                        "type": "object",
                        "enabled": True
                    },

                    "dense_embeddings": {
                        "type": "dense_vector",
                        "dims": 1024,
                        "index": True,
                        "similarity": "cosine",
                        "index_options": {
                            "type": "hnsw"
                        }
                    },

                    "sparse_embeddings": {
                        "type": "rank_features"
                    },

                    "created_at": {
                        "type": "date"
                    },

                    "updated_at": {
                        "type": "date"
                    }
                }
            }
        }

        if not self.es.indices.exists(index=self.es_index_name):

            self.es.indices.create(index=self.es_index_name, body=mapping)
            print(f"Index created: {self.es_index_name}")

        else:
            print(f"Index already exists: {self.es_index_name}")

    # =========================================================
    # Sparse Embedding Convert
    # =========================================================

    @staticmethod
    def convert_sparse_embedding(sparse_json):
        """
        PostgreSQL JSONB → Elasticsearch rank_features 변환
        """

        if sparse_json is None:
            return {}

        result = {}

        for key, value in sparse_json.items():

            try:
                result[f"t_{key}"] = float(value)

            except Exception:
                continue

        return result

    # =========================================================
    # PostgreSQL Fetch
    # =========================================================

    def fetch_data(
        self,
        batch_size: int = 100
        ):
        """
        PostgreSQL 데이터 batch fetch
        """

        conn = self._get_pg_connection()

        query = f"""
            SELECT
                id,
                hashed_content,
                page_content,
                metadata,
                dense_embeddings,
                sparse_embeddings,
                created_at,
                updated_at
            FROM {self.table_name}
        """

        try:

            with conn.cursor() as cur:

                cur.execute(query)

                while True:

                    rows = cur.fetchmany(batch_size)

                    if not rows:
                        break

                    yield rows

        finally:
            conn.close()

    # =========================================================
    # Elasticsearch Action Generator
    # =========================================================
    def _flatten_dict(self, d, parent_key="", sep="_"):
        """
        중첩 딕셔너리를 1차원 딕셔너리로 변환
        """

        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def generate_actions(self, rows):
        """
        Elasticsearch bulk action generator
        """

        for row in rows:
            (
                id_,
                hashed_content,
                page_content,
                metadata,
                dense_embeddings,
                sparse_embeddings,
                created_at,
                updated_at
            ) = row
            # pgvector 처리
            if isinstance(dense_embeddings, str):
                try:
                    dense_embeddings = json.loads(
                        dense_embeddings
                    )
                except Exception:
                    dense_embeddings = []

            action = {
                "_index": self.es_index_name,
                "_id": id_,

                "_source": {
                    "id": id_,
                    "hashed_content": hashed_content,
                    "page_content": page_content,
                    "metadata": self._flatten_dict(metadata) or {},
                    "dense_embeddings": dense_embeddings,
                    "sparse_embeddings":
                        self.convert_sparse_embedding(
                            sparse_embeddings
                        ),
                    "created_at": created_at,
                    "updated_at": updated_at
                }
            }

            yield action

    # =========================================================
    # Bulk Index
    # =========================================================

    def bulk_index(self, batch_size: int = 200, chunk_size: int = 200):
        """
        PostgreSQL → Elasticsearch Bulk Index
        """

        total_indexed = 0

        for rows in self.fetch_data(batch_size=batch_size):

            actions = self.generate_actions(rows)

            success, failed = helpers.bulk(
                self.es,
                actions,
                chunk_size=chunk_size,
                request_timeout=300
            )

            total_indexed += success

            logger.info(f"Indexed: {total_indexed}")

        logger.info("Bulk indexing completed")

    # =========================================================
    # Hybrid Search
    # =========================================================

    def hybrid_search(
        self,
        query: str,
        query_vector: list,
        size: int = 10
        ):
        """
        BM25 + Dense Vector Hybrid Search
        """

        response = self.es.search(
            index=self.es_index_name,

            body={
                "size": size,

                "query": {

                    "script_score": {

                        "query": {
                            "match": {
                                "page_content": query
                            }
                        },

                        "script": {

                            "source": """
                                cosineSimilarity(
                                    params.query_vector,
                                    'dense_embeddings'
                                ) + 1.0
                            """,

                            "params": {
                                "query_vector": query_vector
                            }
                        }
                    }
                }
            }
        )

        return response

    # =========================================================
    # Delete Index
    # =========================================================

    def delete_index(self):

        if self.es.indices.exists(index=self.es_index_name):

            self.es.indices.delete(index=self.es_index_name)
            logger.info(f"Deleted index: {self.es_index_name}")

    # =========================================================
    # Count
    # =========================================================

    def count_documents(self):
        response = self.es.count(index=self.es_index_name)
        return response["count"]


# =============================================================
# Example
# =============================================================

if __name__ == "__main__":
    pass