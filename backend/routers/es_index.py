from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from typing import List, Optional

from process.elasticsearch_index import ElasticsearchIndexer

from utils.config import get_config
from utils.setlogger import setup_logger
config = get_config()
logger = setup_logger(f"{__name__}", level=config.LOG_LEVEL)

es_api = APIRouter(prefix="/es", tags=["Elasticsearch"])

# =========================================================
# Request Models
# =========================================================

class CreateIndexRequest(BaseModel):
    index_name: str = Field(..., description="생성할 Elasticsearch 인덱스 이름")

class BulkIndexRequest(BaseModel):
    schema_table_name: str = Field(..., description="스키마명 포함한 테이블명")
    index_name: str = Field(..., description="데이터를 적재할 Elasticsearch 인덱스 이름")
    batch_size: int = Field(200, description="DB에서 한 번에 가져올 배치 크기 (bulk insert 단위)")
    chunk_size: int = Field(200, description="문서를 chunking 할 때 사용할 분할 크기")

class HybridSearchRequest(BaseModel):
    index_name: str = Field(..., description="검색 대상 Elasticsearch 인덱스 이름")
    query: str = Field(..., description="BM25 + Vector hybrid search에 사용할 검색 쿼리")
    size: int = Field(10, description="검색 결과 반환 개수")

# =========================================================
# dependency injection
# =========================================================

def get_es_indexer():
    return ElasticsearchIndexer()

# =========================================================
# API Endpoints
# =========================================================

@es_api.get("/indices", summary="모든 Elasticsearch 인덱스 조회")
def get_all_indices(es_indexer: ElasticsearchIndexer = Depends(get_es_indexer)):

    try:

        indices = es_indexer.get_all_index_names()

        return {
            "count": len(indices),
            "indices": indices
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )


@es_api.post("/indices", summary="Elasticsearch 인덱스 생성")
def create_index(request: CreateIndexRequest, es_indexer: ElasticsearchIndexer = Depends(get_es_indexer)):

    try:
        return es_indexer.create_index(
            index_name=request.index_name
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )


@es_api.delete("/indices/{index_name}", summary="Elasticsearch 인덱스 삭제")
def delete_index(index_name: str, es_indexer: ElasticsearchIndexer = Depends(get_es_indexer)):

    try:
        return es_indexer.delete_index(
            index_name=index_name
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )


@es_api.get("/indices/{index_name}/count", summary="문서 개수 조회")
def count_documents(index_name: str, es_indexer: ElasticsearchIndexer = Depends(get_es_indexer)):

    try:

        count = es_indexer.count_documents(
            index_name=index_name
        )

        return {
            "index_name": index_name,
            "document_count": count
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )


@es_api.post("/bulk-index", summary="PostgreSQL → Elasticsearch Bulk Index")
def bulk_index(request: BulkIndexRequest, es_indexer: ElasticsearchIndexer = Depends(get_es_indexer)):

    try:

        result = es_indexer.bulk_index(
            schema_table_name=request.schema_table_name,
            index_name=request.index_name,
            batch_size=request.batch_size,
            chunk_size=request.chunk_size
        )

        return result

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )


@es_api.post("/hybrid-search", summary="Hybrid Search")
def hybrid_search(request: HybridSearchRequest, es_indexer: ElasticsearchIndexer = Depends(get_es_indexer)):

    try:

        response = es_indexer.hybrid_search(
            index_name=request.index_name,
            query=request.query,
            size=request.size
        )

        return response

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )