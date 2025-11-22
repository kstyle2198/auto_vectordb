# from fastapi import APIRouter, HTTPException, UploadFile, File, Form, HTTPException
# from pydantic import BaseModel, Field
# from langchain_ollama import OllamaEmbeddings
# from typing import List, Optional, Dict, Any
# from process.elasticsearch_index import ElasticsearchIndexer # indexer.py 파일에 클래스가 있다고 가정

# # --- 설정 및 초기화 ---

# # ElasticsearchIndexer 인스턴스 초기화
# # 실제 환경에 맞게 es_url과 index_name을 설정하세요.
# try:
#     # 이 인스턴스는 FastAPI 애플리케이션이 시작될 때 한 번만 생성됩니다.
#     es_indexer = ElasticsearchIndexer(
#         es_url="http://localhost:9200", 
#         index_name="test_002"
#         )
# except Exception as e:
#     # ES 연결 실패 시 초기화 오류 처리 (필요에 따라 더 상세히 구현)
#     print(f"Elasticsearch Indexer initialization failed: {e}")
#     es_indexer = None # 인덱서가 초기화되지 않으면 None으로 설정

# es_api = APIRouter()
# embed_model = OllamaEmbeddings(base_url="http://localhost:11434", model="bge-m3:latest")

# # --- Pydantic 스키마 정의 ---

# # 색인 요청을 위한 입력 모델
# class IndexRequest(BaseModel):
#     table_name: str
#     hashed_filepath: str

# # 문서 조회 응답 모델
# class DocumentResponse(BaseModel):
#     # 실제 문서의 필드에 맞게 조정해야 합니다.
#     id: str
#     page_content: str
#     filename: str
#     filepath: str
#     # ... 기타 필드 ...
#     # 응답 스키마가 복잡하다면, 그냥 dict로 반환하고 스키마 검증을 생략할 수도 있습니다.
#     class Config:
#         extra = "allow" # 예상치 못한 필드가 있어도 허용하도록 설정

# # --- 엔드포인트 정의 ---

# @es_api.post("/index/document", tags=["ElasticSearch"])
# def index_document_by_path(request: IndexRequest):
#     """
#     주어진 table_name과 hashed_filepath를 사용하여 문서를 PostgreSQL에서 가져와
#     Elasticsearch에 색인합니다.
#     """
#     if es_indexer is None:
#         raise HTTPException(status_code=503, detail="Service Unavailable: Elasticsearch Indexer failed to initialize.")

#     try:
#         es_indexer.index_documents_by_hashed_filepath(
#             table_name=request.table_name,
#             hashed_filepath=request.hashed_filepath
#         )
#         return {"message": f"Indexing request sent for hashed_filepath: {request.hashed_filepath}"}
#     except Exception as e:
#         # 색인 중 발생한 예외 처리 (e.g., PostgreSQL 연결 오류 등)
#         # 실제 index_documents_by_hashed_filepath 메서드에서 이미 로깅을 하고 있으므로,
#         # 여기서는 일반적인 HTTP 500 오류를 반환합니다.
#         print(f"Indexing error: {e}")
#         raise HTTPException(status_code=500, detail=f"Failed to index document: {e}")

# @es_api.get("/document/{hashed_filepath}", tags=["ElasticSearch"])
# def get_document(hashed_filepath: str):
#     """
#     주어진 ID에 해당하는 문서를 Elasticsearch에서 조회합니다.
#     (doc_id는 Elasticsearch 문서 ID, 즉 hashed_filepath와 동일)
#     """
#     if es_indexer is None:
#         raise HTTPException(status_code=503, detail="Service Unavailable: Elasticsearch Indexer failed to initialize.")

#     document = es_indexer.search_documents_by_hashed_filepath(hashed_filepath=hashed_filepath)
    
#     if document is None:
#         # get_document_by_id에서 문서가 없으면 None을 반환하므로, 404 처리
#         raise HTTPException(status_code=404, detail=f"Document with ID '{hashed_filepath}' not found.")
    
#     # DocumentResponse 스키마에 맞지 않아도 일단 반환 (dict)
#     # 실제로는 DocumentResponse 스키마에 맞춰 검증하고 반환하는 것이 좋습니다.
#     return document


# class SearchRequest(BaseModel):
#     """검색 요청 본문에 포함될 데이터 모델"""
#     # 텍스트 검색어 (선택 사항)
#     query_text: Optional[str] = Field(None, description="키워드 기반 검색을 위한 텍스트 검색어.")
#     # 검색 파라미터
#     size: int = Field(5, ge=1, le=100, description="반환할 최대 문서 수.")
#     min_score: float = Field(0.5, ge=0.0, le=1.0, description="결과에 포함될 최소 관련성 점수.")


# @es_api.post("/search", tags=["ElasticSearch"])
# async def search_documents_endpoint(request: SearchRequest):
#     """
#     Elasticsearch에서 텍스트 또는 임베딩을 기반으로 문서를 검색합니다.

#     **query_text** 또는 **query_embedding** 중 최소 하나를 제공해야 합니다.
#     """
    
#     query_text = request.query_text
#     query_embedding = embed_model.embed_query(query_text)
#     size = request.size
#     min_score = request.min_score

#     # 필수 조건 검사
#     if not query_text and not query_embedding:
#         raise HTTPException(
#             status_code=400,
#             detail="검색을 위해서는 'query_text' 또는 'query_embedding' 중 최소 하나를 제공해야 합니다."
#         )

#     # 1024 차원 벡터 길이 검증 (임베딩이 제공된 경우)
#     if query_embedding and len(query_embedding) != 1024:
#         raise HTTPException(
#             status_code=400,
#             detail=f"제공된 임베딩 벡터의 차원이 {len(query_embedding)}입니다. 1024 차원 벡터가 필요합니다."
#         )

#     try:
#         # 2. ElasticsearchIndexer의 search_documents 메서드 호출
#         results = es_indexer.search_documents(
#             query_text=query_text,
#             query_embedding=query_embedding,
#             size=size,
#             min_score=min_score
#             )
        
#         # 3. 결과 반환
#         return {
#             "query_text": query_text,
#             "query_type": "Hybrid" if query_text and query_embedding else ("Text" if query_text else "Vector"),
#             "total_hits": len(results),
#             "results": results
#         }

#     except Exception as e:
#         # 검색 중 발생한 예외 처리
#         print(f"Search error: {e}")
#         raise HTTPException(
#             status_code=500,
#             detail=f"검색 중 서버 오류가 발생했습니다: {e}"
#         )


from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from langchain_ollama import OllamaEmbeddings
from typing import Optional
from process.elasticsearch_index import ElasticsearchIndexer

es_api = APIRouter()
embed_model = OllamaEmbeddings(base_url="http://localhost:11434", model="bge-m3:latest")


# --- 공통: 요청마다 동적으로 Indexer 생성 ---
def create_es_indexer(index_name: str) -> ElasticsearchIndexer:
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
