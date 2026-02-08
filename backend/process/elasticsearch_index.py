import numpy as np
from process.postgres import PostgresPipeline
from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import BulkIndexError
from elasticsearch import NotFoundError

# 설정 및 로거 로드 (기존 코드를 따름)
from utils.config import get_config
from utils.setlogger import setup_logger
config = get_config()
logger = setup_logger(f"{__name__}", level=config.LOG_LEVEL)

class ElasticsearchIndexer:
    """
    Elasticsearch 연결, 인덱스 관리 및 데이터 색인 작업을 캡슐화하는 클래스입니다.
    """

    def __init__(self, es_url: str = "http://localhost:9200", index_name: str = "test_002"):
        """
        ElasticsearchIndexer 클래스를 초기화하고 Elasticsearch 연결 및 인덱스를 설정합니다.
        """
        self.INDEX_NAME = index_name
        self.es = Elasticsearch(es_url)
        self.pg_pipe = PostgresPipeline() # PostgreSQL 파이프라인 인스턴스 (가정)
        self.mapping = {
            "mappings": {
                "properties": {
                    "id": { "type": "keyword" },
                    "page_content": { "type": "text" },
                    "filename": { "type": "keyword" },
                    "filepath": { "type": "keyword" },
                    "hashed_filename": { "type": "keyword" },
                    "hashed_filepath": { "type": "keyword" },
                    "hashed_page_content": { "type": "keyword" },
                    "page": { "type": "keyword" },
                    "lv1_cat": { "type": "keyword" },
                    "lv2_cat": { "type": "keyword" },
                    "lv3_cat": { "type": "keyword" },
                    "lv4_cat": { "type": "keyword" },
                    "embeddings": {
                        "type": "dense_vector",
                        "dims": 2560
                    },
                    "created_at": { "type": "date" },
                    "updated_at": { "type": "date" }
                }
            }
        }
        
        self._ensure_index_exists()

    def _ensure_index_exists(self):
        """
        Elasticsearch 인덱스가 존재하지 않으면 생성합니다.
        """
        if not self.es.indices.exists(index=self.INDEX_NAME):
            try:
                self.es.indices.create(index=self.INDEX_NAME, body=self.mapping)
                logger.info(f"Elasticsearch Index '{self.INDEX_NAME}' created successfully.")
            except Exception as e:
                logger.error(f"Error creating index '{self.INDEX_NAME}': {e}")
                # 인덱스 생성 실패 시 추가적인 에러 처리가 필요할 수 있습니다.
        else:
            logger.info(f"Index '{self.INDEX_NAME}' already exists.")


    @staticmethod
    def _convert_numpy_types(obj):
        """
        재귀적으로 numpy 타입을 Python 기본 타입으로 변환 (정적 메서드로 유지)
        """
        if isinstance(obj, dict):
            return {k: ElasticsearchIndexer._convert_numpy_types(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [ElasticsearchIndexer._convert_numpy_types(x) for x in obj]
        elif isinstance(obj, np.ndarray):
            return obj.astype(float).tolist()
        elif isinstance(obj, (float, np.ndarray)) or np.isrealobj(obj):
            return float(obj)
        elif isinstance(obj, (int, np.ndarray)) or np.isrealobj(obj):
            return int(obj)
        else:
            return obj

    @staticmethod
    def _parse_embedding_string(val):
        """
        PostgreSQL에서 저장된 embedding 문자열을 float 리스트로 변환 (정적 메서드로 유지)
        """
        if not val:
            return []
        
        # 이미 list라면 그냥 사용
        if isinstance(val, list):
            try:
                 return [float(x) for x in val]
            except ValueError:
                 return [] # 변환 실패 시 빈 리스트 반환

        # 문자열 처리: '{-0.07,...}' → [-0.07,...]
        if isinstance(val, str):
            val = val.strip("{}")
            try:
                return [float(x) for x in val.split(",") if x.strip()]
            except ValueError:
                return []

        # numpy array 처리
        if isinstance(val, np.ndarray):
            return val.astype(float).tolist()

        return []

    def _generate_actions(self, rows):
        """
        Elasticsearch의 helpers.bulk()를 위한 액션을 생성하는 제너레이터입니다.
        """
        for r in rows:
            # PostgreSQL 쿼리 결과의 인덱스에 매핑되는 필드
            doc = {
                "id": r[0],
                "page_content": r[1],
                "filename": r[2],
                "filepath": r[3],
                "hashed_filename": r[4],
                "hashed_filepath": r[5],
                "hashed_page_content": r[6],
                "page": r[7],
                "lv1_cat": r[8],
                "lv2_cat": r[9],
                "lv3_cat": r[10],
                "lv4_cat": r[11],
                "embeddings": self._parse_embedding_string(r[12]),
                "created_at": r[13],
                "updated_at": r[14],
            }
            # numpy 타입 안전 변환
            doc = self._convert_numpy_types(doc)

            yield {
                "_index": self.INDEX_NAME,
                "_id": r[0], 
                "_source": doc
            }

    def index_documents_by_hashed_filepath(self, table_name: str, hashed_filepath: str):
        """
        주어진 hashed_filepath에 해당하는 문서를 PostgreSQL에서 가져와 Elasticsearch에 색인합니다.
        """
        logger.info(f"Fetching rows for hashed_filepath: {hashed_filepath}")
        
        # PostgreSQLPipeline을 통해 데이터 가져오기 (PostgresPipeline 메서드 가정)
        rows = self.pg_pipe.get_row_by_hashed_filepath(table_name=table_name, hashed_filepath=hashed_filepath)

        if not rows:
            logger.warning(f"No rows found for hashed_filepath: {hashed_filepath} in table: {table_name}")
            return

        try:
            # helpers.bulk를 사용하여 문서 일괄 색인
            successes, errors = helpers.bulk(self.es, self._generate_actions(rows), raise_on_error=False)
            # errors가 리스트 형태이고 비어있지 않은지 확인
            if isinstance(errors, list) and errors:
                 logger.warning(f"{len(errors)} document(s) failed to index. First error: {errors[0]}")

            logger.info(f"Successfully indexed {successes} out of {len(rows)} documents to Elasticsearch.")
            
        except BulkIndexError as e:
            # raise_on_error=False로 설정했으므로 이 예외는 발생하지 않을 수 있지만,
            # 혹시 모르니 남겨둡니다.
            logger.error(f"A general BulkIndexError occurred: {e.errors}")
        except Exception as e:
            logger.error(f"An unexpected error occurred during indexing: {e}")

    def search_documents_by_hashed_filepath(self, hashed_filepath: str):
        """
        Elasticsearch에서 주어진 hashed_filepath에 해당하는 모든 문서를 검색합니다.
        이는 하나의 파일(경로)에 속한 모든 페이지/청크를 가져오는 데 사용됩니다.
        """
        
        # 1. Elasticsearch 쿼리 정의
        # 'keyword' 타입 필드에 대해 정확히 일치하는 값을 찾기 위해 'term' 쿼리를 사용합니다.
        search_body = {
            "query": {
                "term": {
                    "hashed_filepath": hashed_filepath # hashed_filepath 필드에 대한 정확한 일치 검색
                }
            },
            "size": 10000  # 한 번에 가져올 수 있는 최대 문서 수 (필요에 따라 조정)
        }
        
        logger.info(f"Searching documents for hashed_filepath: {hashed_filepath}")
        
        try:
            # 2. 검색 실행
            res = self.es.search(index=self.INDEX_NAME, body=search_body)
            
            # 3. 결과 파싱 및 반환
            hits = res['hits']['hits']
            documents = [hit['_source'] for hit in hits]
            
            logger.info(f"Found {len(documents)} documents for hashed_filepath: {hashed_filepath}")
            
            return documents
        
        except NotFoundError:
            # 인덱스가 존재하지 않는 경우
            logger.error(f"Index '{self.INDEX_NAME}' not found.")
            return []
        except Exception as e:
            logger.error(f"Error searching documents by hashed_filepath: {e}")
            return []


# 👇️ 요청하신 검색 메서드 추가
    def search_documents(self, query_text: str = "", query_embedding: list = [], size: int = 10, min_score: float = 0.5):
        """
        Elasticsearch에서 텍스트 또는 임베딩을 기반으로 문서를 검색합니다.
        
        텍스트 검색 (query_text)과 벡터 검색 (query_embedding) 중 하나 또는 둘 다를 사용하여 검색할 수 있습니다.
        둘 다 제공되면 부스트 값이 적용된 Bool 쿼리 (RRF)를 구성합니다.
        
        Args:
            query_text (str, optional): 일반 텍스트 검색어. Defaults to None.
            query_embedding (list, optional): 벡터 검색을 위한 4096차원 임베딩 리스트. Defaults to None.
            size (int, optional): 반환할 최대 문서 수. Defaults to 10.
            min_score (float, optional): 최소 점수 임계값. Defaults to 0.5.
            
        Returns:
            list: 검색 결과 문서 (hit['_source'] + score) 리스트.
        """
        if not query_text and not query_embedding:
            logger.warning("Either query_text or query_embedding must be provided.")
            return []

        search_body = {
            "size": size,
            "min_score": min_score,
            "query": {
                "bool": {
                    "should": [],
                    "minimum_should_match": 1 # 'should' 절 중 최소 하나는 일치해야 함
                }
            },
            # Elasticsearch 8.x 이상에서 kNN 검색을 위한 kNN 섹션 추가 (Elasticsearch 버전에 따라 달라질 수 있음)
            "knn": []
        }
        
        # 1. 일반 텍스트 검색 쿼리 (Query Text)
        if query_text:
            # "page_content" 필드에서 텍스트를 검색하는 match 쿼리 추가
            search_body["query"]["bool"]["should"].append({
                "match": {
                    "page_content": {
                        "query": query_text,
                        "boost": 1.0 # 텍스트 검색 부스트 값
                    }
                }
            })
            logger.info(f"Text search enabled for: {query_text}")
        
        # 2. 벡터 검색 쿼리 (Query Embedding)
        if query_embedding:
            if len(query_embedding) != 4096:
                logger.error(f"Embedding must be 4096 dimensions, got {len(query_embedding)}")
                return []
            
            # kNN 섹션에 dense_vector 검색 추가
            # 참고: Elasticsearch 8.x 버전에서는 search API의 'knn' 파라미터를 사용하거나
            # 7.x 버전에서는 'script_score' 쿼리를 사용할 수 있습니다.
            # 여기서는 8.x의 'knn' 파라미터를 사용하는 표준 방식을 따릅니다.
            search_body["knn"].append({
                "field": "embeddings",
                "query_vector": query_embedding,
                "k": size, # k: 이웃 수
                "num_candidates": max(size * 10, 50), # 검색할 후보 수 (성능/정확도 트레이드오프)
                "boost": 0.8 # 벡터 검색 부스트 값 (텍스트 검색보다 약간 낮게 설정)
            })
            
            # kNN을 사용할 경우, 최소 점수 대신 필터링을 사용하여 관련 없는 문서를 제거할 수 있습니다.
            # 이 예시에서는 min_score를 유지합니다.
            
            logger.info("Vector search enabled.")
            
        try:
            # 3. 검색 실행
            # Elasticsearch 8.x에서는 kNN과 쿼리를 조합할 수 있습니다.
            # 'knn' 파라미터가 비어 있지 않으면 'search_body'에서 'knn'을 제거하고 별도의 'knn' 인수로 전달해야 합니다.
            # 하지만 8.x 클라이언트의 search 메서드가 body에 knn을 허용하는 경우가 많으므로 body에 포함합니다.
            res = self.es.search(
                index=self.INDEX_NAME, 
                body=search_body
            )
            
            # 4. 결과 파싱 및 반환
            hits = res['hits']['hits']
            
            # 결과에 점수 (Relevance Score)를 포함하여 반환합니다.
            documents = [{'_score': hit['_score'], **hit['_source']} for hit in hits]
            
            logger.info(f"Found {len(documents)} documents.")
            
            return documents
            
        except Exception as e:
            logger.error(f"Error searching documents: {e}")
            return []
        
    def get_all_index_names(self):
        """
        Elasticsearch 클러스터에 존재하는 모든 인덱스의 이름을 리스트로 반환합니다.
        """
        try:
            # indices.get_alias("*")는 모든 인덱스의 별칭 정보를 가져오며, 
            # 딕셔너리의 키(key)가 인덱스 이름입니다.
            indices_dict = self.es.indices.get_alias(index="*")
            index_names = list(indices_dict.keys())
            
            logger.info(f"Retrieved {len(index_names)} indices from Elasticsearch.")
            return indices_dict
            
        except Exception as e:
            logger.error(f"Error fetching all index names: {e}")
            return []
        
    def get_index_names_by_prefix(self, prefix: str) -> list[str]:
        """
        Elasticsearch 클러스터에서 특정 prefix를 가진 인덱스 이름만 반환합니다.
        """
        try:
            # ES 단에서 prefix 필터링
            indices_dict = self.es.indices.get_alias(index=f"{prefix}*")
            index_names = list(indices_dict.keys())

            logger.info(
                f"Retrieved {len(index_names)} indices with prefix '{prefix}'."
            )
            return index_names

        except Exception as e:
            logger.error(
                f"Error fetching index names with prefix '{prefix}': {e}"
            )
            return []

    def delete_index_by_name(self, index_name: str):
        """
        지정된 이름의 인덱스를 삭제합니다.
        
        Args:
            index_name (str): 삭제할 인덱스의 이름
        
        Returns:
            bool: 삭제 성공 시 True, 실패하거나 존재하지 않으면 False
        """
        if not index_name:
            logger.warning("No index name provided for deletion.")
            return False

        try:
            if self.es.indices.exists(index=index_name):
                self.es.indices.delete(index=index_name)
                logger.info(f"Index '{index_name}' has been deleted successfully.")
                return True
            else:
                logger.warning(f"Index '{index_name}' does not exist, skipping deletion.")
                return False
                
        except Exception as e:
            logger.error(f"Error deleting index '{index_name}': {e}")
            return False

if __name__ == "__main__":
    
    es = ElasticsearchIndexer()
    test_key = "5476ca42f4dd6e62009b59289f1c7f84"  # 예시 hashed_filepath
    # es.index_by_hashed_filepath(INDEX_NAME, test_key)
    # pass

    # 2. 특정 패턴을 가진 인덱스 이름만 리스트로 추출

  
    index_list = es.get_all_index_names()
    print(index_list)