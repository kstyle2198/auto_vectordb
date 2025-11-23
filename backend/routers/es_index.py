from fastapi import APIRouter, HTTPException, status, Path
from pydantic import BaseModel, Field
from langchain_ollama import OllamaEmbeddings
from elasticsearch import Elasticsearch
from typing import Optional, List
from process.elasticsearch_index import ElasticsearchIndexer

es_api = APIRouter()
es = Elasticsearch("http://localhost:9200")
embed_model = OllamaEmbeddings(base_url="http://localhost:11434", model="bge-m3:latest")


# --- 공통: 요청마다 동적으로 Indexer 생성 ---
def create_es_indexer(index_name: str="test_01") -> ElasticsearchIndexer:
    try:
        return ElasticsearchIndexer(
            es_url="http://localhost:9200",
            index_name=index_name
        )
    except Exception as e:
        print(f"Elasticsearch Indexer initialization failed: {e}")
        raise HTTPException(status_code=503, detail="Elasticsearch 연결 실패")


# --- Pydantic 스키마 ---

class IndexRequest(BaseModel):
    index_name: str = Field(..., description="Elasticsearch 인덱스명")
    table_name: str
    hashed_filepath: str


class DocumentResponse(BaseModel):
    id: str
    page_content: str
    filename: str
    filepath: str
    class Config:
        extra = "allow"


class SearchRequest(BaseModel):
    index_name: str = Field(..., description="검색할 Elasticsearch 인덱스명")
    query_text: Optional[str] = None
    size: int = Field(5, ge=1, le=100)
    min_score: float = Field(0.5, ge=0.0, le=1.0)


# --- 엔드포인트 ---

@es_api.post("/index/document", tags=["ElasticSearch"])
def index_document_by_path(request: IndexRequest):
    """
    index_name + table_name + hashed_filepath → 문서 색인
    """
    es_indexer = create_es_indexer(index_name=request.index_name)

    try:
        es_indexer.index_documents_by_hashed_filepath(
            table_name=request.table_name,
            hashed_filepath=request.hashed_filepath
        )
        return {
            "message": f"[{request.index_name}] 인덱싱 요청 완료",
            "hashed_filepath": request.hashed_filepath
        }
    except Exception as e:
        print(f"Indexing error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to index: {e}")


@es_api.get("/document/{index_name}/{hashed_filepath}", tags=["ElasticSearch"])
def get_document(index_name: str, hashed_filepath: str):
    """
    index_name + hashed_filepath → 문서 조회
    """
    es_indexer = create_es_indexer(index_name=index_name)

    document = es_indexer.search_documents_by_hashed_filepath(hashed_filepath)

    if document is None:
        raise HTTPException(
            status_code=404,
            detail=f"[{index_name}] 문서 '{hashed_filepath}' 없음"
        )

    return document


@es_api.post("/search", tags=["ElasticSearch"])
async def search_documents_endpoint(request: SearchRequest):
    """
    index_name에서 query_text/embedding 기반 검색
    """
    es_indexer = create_es_indexer(index_name=request.index_name)

    # ---- 검색 키워드 처리 ----
    query_text = request.query_text
    if not query_text:
        raise HTTPException(
            status_code=400,
            detail="query_text는 필수입니다."
            )

    query_embedding = embed_model.embed_query(query_text)

    if len(query_embedding) != 1024:
        raise HTTPException(
            status_code=400,
            detail=f"임베딩 차원 오류: {len(query_embedding)} (1024 필요)"
            )

    # ---- 실제 ES 검색 ----
    try:
        results = es_indexer.search_documents(
            query_text=query_text,
            query_embedding=query_embedding,
            size=request.size,
            min_score=request.min_score
        )

        return {
            "index_name": request.index_name,
            "query_text": query_text,
            "total_hits": len(results),
            "results": results
        }

    except Exception as e:
        print(f"Search error: {e}")
        raise HTTPException(status_code=500, detail=f"검색 오류: {e}")
    

# --- Pydantic 모델 정의 (API 문서화 및 응답 형식을 위해) ---
class IndexListResponse(BaseModel):
    count: int
    indices: List[str]

class DeleteIndexResponse(BaseModel):
    index_name: str
    deleted: bool
    message: str

# --- Endpoints ---

@es_api.get(
    "/indices", 
    response_model=IndexListResponse,
    summary="전체 인덱스 목록 조회",
    description="Elasticsearch 클러스터에 존재하는 모든 인덱스의 이름을 조회합니다.", tags=["ElasticSearch"]
    )
async def get_all_indices():
    """
    모든 Elasticsearch 인덱스 이름을 조회하여 반환합니다.
    """
    es_indexer = create_es_indexer()
    indices_dict = es_indexer.get_all_index_names()
    # indices_dict = es.indices.get_alias(index="*")
    print(indices_dict)
    index_names = list(indices_dict.keys())
    
    return {
        "count": len(index_names),
        "indices": indices_dict
        }

@es_api.delete(
    "/indices/{index_name}", 
    response_model=DeleteIndexResponse,
    summary="특정 인덱스 삭제",
    description="지정된 이름의 Elasticsearch 인덱스를 영구적으로 삭제합니다.", tags=["ElasticSearch"]
    )
async def delete_index(
    index_name: str = Path(..., description="삭제할 인덱스의 이름", example="test_002")
    ):
    """
    특정 인덱스를 삭제합니다.
    
    - **index_name**: 삭제할 인덱스 명
    """
    es_indexer = create_es_indexer(index_name=index_name)
    # 인덱스 삭제 시도
    is_deleted = es_indexer.delete_index_by_name(index_name)
    
    if is_deleted:
        return {
            "index_name": index_name,
            "deleted": True,
            "message": f"Index '{index_name}' successfully deleted."
        }
    else:
        # 삭제 실패 (인덱스가 없거나 에러 발생) 시 404 에러 반환
        # 보안 정책에 따라 에러 메시지를 다르게 줄 수 있습니다.
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Index '{index_name}' not found or could not be deleted."
        )
