import time
import spacy
import numpy as np
from tqdm import tqdm
from rank_bm25 import BM25Okapi
from elasticsearch import Elasticsearch
from langchain_ollama import OllamaEmbeddings

class HybridSearch:
    def __init__(
        self, 
        es_url: str = "http://localhost:9200", 
        ollama_url: str = "http://localhost:11434", 
        model_name: str = "qwen3-embedding:4b",
        spacy_model_path: str = "D:\\models\\en_core_web_sm"
    ):
        # 1. 초기화: ES, Embedding, NLP 모델 로드
        self.es = Elasticsearch(es_url)
        self.embed_model = OllamaEmbeddings(base_url=ollama_url, model=model_name)
        self.nlp = spacy.load(spacy_model_path, disable=["ner", "parser"])
        print(f"HybridSearch initialized with model: {model_name}")

    def _spacy_tokenizer(self, text: str):
        """텍스트 전처리 및 토큰화"""
        doc = self.nlp(text)
        return [token.lemma_.lower() for token in doc 
                if not token.is_stop and not token.is_punct and not token.is_space]

    def _sigmoid(self, x, scaling_factor=10):
        """BM25 점수 정규화"""
        return 1 / (1 + np.exp(-np.array(x) / scaling_factor))

    def semantic_search(
        self, 
        index_names: list, 
        query_text: str, 
        size: int = 10, 
        similarity_threshold: float = 0.5,
        lv1_cat: str = "", 
        start_date: str = "", 
        end_date: str = ""
    ):
        """Elasticsearch Vector(kNN) + 필터 검색"""
        meta_filters = []
        if lv1_cat:
            meta_filters.append({"term": {"lv1_cat": lv1_cat}})
        
        if start_date or end_date:
            date_range = {"range": {"created_at": {}}}
            if start_date: date_range["range"]["created_at"]["gte"] = start_date
            if end_date: date_range["range"]["created_at"]["lte"] = end_date
            meta_filters.append(date_range)

        search_body = {
            "size": size,
            "query": {
                "bool": {
                    "must": [{"match": {"page_content": {"query": query_text, "boost": 1.0}}}],
                    "filter": meta_filters
                }
            }
        }

        query_embedding = self.embed_model.embed_query(query_text)
        if query_embedding:
            search_body["knn"] = {
                "field": "embeddings",
                "query_vector": query_embedding,
                "k": size,
                "num_candidates": max(size * 10, 50),
                "boost": 0.8,
                "similarity": similarity_threshold,
                "filter": meta_filters
            }

        try:
            res = self.es.search(index=index_names, body=search_body)
            hits = res['hits']['hits']
            # RRF 계산을 위해 id와 원본 소스를 함께 반환
            return [{**hit['_source'], 'id': hit.get('_id', hit['_source'].get('id'))} for hit in hits]
        except Exception as e:
            print(f"Semantic Search Error: {e}")
            return []

    def bm25_search(self, corpus_docs: list, query: str, top_k: int = 3):
        """BM25 키워드 검색 (제공된 문서 리스트 내에서)
          corpus (list): 검색 대상이 되는 문자열 리스트.
          query (str): 검색할 쿼리 문자열.
          top_k (int, optional): 반환할 상위 문서의 개수. Defaults to 3.
          batch_size (int, optional): spaCy 파이프라인 처리를 위한 배치 크기. Defaults to 5.
          n_process (int, optional): 병렬 처리에 사용할 프로세스 수. Defaults to 2.
        """
        if not corpus_docs:
            return []

        # 1. 말뭉치 토큰화
        corpus_texts = [doc["page_content"] for doc in corpus_docs]
        tokenized_corpus = []
        for doc in self.nlp.pipe(corpus_texts, batch_size=10, n_process=1):
            tokens = [token.lemma_.lower() for token in doc if not token.is_stop and not token.is_punct]
            tokenized_corpus.append(tokens)

        # 2. BM25 계산
        bm25 = BM25Okapi(tokenized_corpus)
        tokenized_query = self._spacy_tokenizer(query)
        doc_scores = bm25.get_scores(tokenized_query)
        
        # 3. 결과 구성
        top_n_indices = np.argsort(doc_scores)[::-1][:top_k]
        results = []
        for idx in top_n_indices:
            results.append({
                **corpus_docs[idx],
                "bm25_score": np.round(doc_scores[idx], 4),
                "bm25_similarity": np.round(self._sigmoid(doc_scores[idx]), 4)
            })
        return results

    def rrf_fusion(self, results_list: list, k: int = 60):
        """Reciprocal Rank Fusion 알고리즘"""
        rrf_scores = {}
        for rank_list in results_list:
            for rank, doc in enumerate(rank_list):
                doc_id = doc.get('id')
                if not doc_id: continue
                
                if doc_id not in rrf_scores:
                    rrf_scores[doc_id] = {"score": 0, "doc": doc}
                
                rrf_scores[doc_id]["score"] += 1 / (k + rank + 1)

        # 점수 순 정렬
        sorted_rrf = sorted(rrf_scores.values(), key=lambda x: x["score"], reverse=True)
        return sorted_rrf

    def search(self, index_names: list, query: str, size: int = 10, **kwargs):
        """최종 하이브리드 검색 실행 메서드"""
        # 1. Semantic 검색 실행 (ES)
        semantic_results = self.semantic_search(index_names, query, size=size, **kwargs)
        print(f">>>semantic_results개수: {len(semantic_results)}")
        
        # 2. BM25 검색 실행 (Semantic 결과 대상)
        bm25_results = self.bm25_search(semantic_results, query, top_k=len(semantic_results))
        print(f">>>bm25_results개수: {len(bm25_results)}")

        # 3. RRF 융합
        final_ranked = self.rrf_fusion([semantic_results, bm25_results])
        print(f">>>final_ranked개수: {len(final_ranked)}")
        
        return final_ranked
    

if __name__ == "__main__":

    # 클래스 인스턴스 생성
    hybrid = HybridSearch()

    start_time = time.time()
    # 하이브리드 검색 실행
    results = hybrid.search(
        index_names=["ai_paper"],
        query="auto recursive regression",
        size=200,
        lv1_cat="Paper",
        start_date="2024-01-01",
        similarity_threshold = 0.3
    )

    end_time = time.time()
    print(f"작업완료 - 총 {len(results)} | {end_time-start_time}초 소요")
    # 결과 출력
    for item in results:
        print(f"Score: {item['score']:.4f} | Content: {item['doc']['page_content'][:50]}...")