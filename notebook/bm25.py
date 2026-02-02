import spacy
import numpy as np
from rank_bm25 import BM25Okapi
from tqdm import tqdm

# 영어 모델 로드 (가벼운 sm 모델 또는 정밀한 trf 모델 선택 가능)
nlp = spacy.load("D:\\models\\en_core_web_sm", disable=["ner", "parser"])

# 3. spaCy 전처리 함수
def spacy_tokenizer(text):
    # 문서 객체 생성
    doc = nlp(text)
    # 1. 불용어(Stopwords) 제거
    # 2. 구두점(Punctuation) 제거 
    # 3. 표제어 추출(Lemmatization) 및 소문자화
    return [token.lemma_.lower() for token in doc 
            if not token.is_stop and not token.is_punct and not token.is_space]

def sigmoid(x, scaling_factor=10):
    """
    BM25 점수를 0~1 사이로 변환합니다.
    scaling_factor가 클수록 점수 차이가 완만하게 반영됩니다.
    """
    return 1 / (1 + np.exp(-np.array(x) / scaling_factor))

def main(corpus: list, query: str, top_k: int = 3):
    # 1. 전처리 및 토큰화
    print("Tokenizing documents...")
    tokenized_corpus = []
    # n_process=-1을 위해 if __name__ == "__main__": 블록 내 실행 권장
    for doc in tqdm(nlp.pipe(corpus, batch_size=2000, n_process=-1), total=len(corpus)):
        tokens = [token.lemma_.lower() for token in doc if not token.is_stop and not token.is_punct]
        tokenized_corpus.append(tokens)

    # 2. BM25 인덱싱
    bm25 = BM25Okapi(tokenized_corpus)

    # 3. 쿼리 전처리 및 점수 계산
    tokenized_query = spacy_tokenizer(query)
    doc_scores = bm25.get_scores(tokenized_query)

    # 4. Sigmoid 정규화 적용
    # 보통 BM25 상위 점수가 10~20 내외이므로 scaling_factor를 10 정도로 잡으면 적절합니다.
    normalized_scores = sigmoid(doc_scores, scaling_factor=10)

    # 5. 상위 K개 인덱스 추출
    top_n_indices = np.argsort(doc_scores)[::-1][:top_k]

    # 6. 결과 구성 (문서 내용, 원본 점수, 정규화 점수)
    results = []
    for idx in top_n_indices:
        results.append({
            "content": corpus[idx],
            "score": np.round(doc_scores[idx], 4),
            "similarity": np.round(normalized_scores[idx], 4)
        })

    return results

if __name__ == "__main__":
    corpus = [
        "The fatty acids in fish are good for heart health.",
        "Data scientists use Python to build machine learning models.",
        "The quick brown fox jumps over the lazy dog.",
        "BM25 is a ranking function used by search engines to estimate the relevance of documents.",
        "Natural language processing involves the interaction between computers and humans."
        ]
    query = "Are fish oils healthy for the heart?"
    res = main(corpus=corpus, query=query, top_k=3)
    print(res)


