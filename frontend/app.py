import os
import json
import requests
import streamlit as st
from pathlib import Path
from tqdm.auto import tqdm

from utils.config import get_config
from utils.setlogger import setup_logger
config = get_config()
logger = setup_logger(f"{__name__}", level=config.LOG_LEVEL)

FASTAPI_BASEURL = "http://localhost:8000"

def list_files_recursive(folder_path: str):
    """폴더 안의 파일을 재귀적으로 읽어서 제너레이터로 반환하는 함수"""
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            yield os.path.join(root, file)

def count_files(folder_path: str) -> int:
    """총 파일 개수 (메모리 부담 없음)"""
    count = 0
    for _, _, files in os.walk(folder_path):
        count += len(files)
    return count

def upload_file_to_backend(local_path: str, server_top_path: str):
    """파일 경로를 받아서 해당 파일을 백엔드로 보내는 함수"""
    try:
        folder_path = os.path.dirname(local_path).replace("\\", "/")
        server_path = f"{server_top_path}{folder_path.replace('\\', '/').replace(local_base_path, '')}"

        with open(local_path, "rb") as f:
            files = {"file": (os.path.basename(local_path), f)}
            data = {"local_path": local_path, "server_path": server_path}

            requests.post(f"{FASTAPI_BASEURL}/upload", files=files, data=data)

        logger.info(f"Uploaded Successfully - {local_path}")

    except Exception as e:
        logger.error(e)

def upload_file_in_chunks(local_path:str, server_top_path:str):
    chunk_size = 10 * 1024 * 1024   # 10MB
    local_filename = os.path.basename(local_path)
    file_size = os.path.getsize(local_path)
    total_chunks = (file_size + chunk_size - 1) // chunk_size

    folder_path = os.path.dirname(local_path).replace("\\", "/")
    server_path = f"{server_top_path}{folder_path.replace('\\', '/').replace(local_base_path, '')}"

    with open(local_path, "rb") as f:
        for chunk_index in range(total_chunks):
            chunk = f.read(chunk_size)

            files = {"file": ("chunk", chunk)}
            data = {
                "filename": local_filename,
                "chunk_index": int(chunk_index),
                "total_chunks": int(total_chunks),
                "server_path": server_path,
                }
            try:
                res = requests.post(f"{FASTAPI_BASEURL}/upload_chunk", files=files, data=data)
                logger.info(f"Chunked file is Uploaded Successfully - {local_path}")
            except Exception as e:
                logger.error(e)


col_schema = [
    {'name': 'id', 'type': 'VARCHAR(300) NOT NULL'}, 
    {'name': 'page_content', 'type': 'TEXT NOT NULL'}, 
    {'name': 'filename', 'type': 'VARCHAR(300) NOT NULL'}, 
    {'name': 'filepath', 'type': 'VARCHAR(300) NOT NULL'}, 
    {'name': 'hashed_filename', 'type': 'VARCHAR(300)'}, 
    {'name': 'hashed_filepath', 'type': 'VARCHAR(300)'}, 
    {'name': 'hashed_page_content', 'type': 'VARCHAR(300)'}, 
    {'name': 'page', 'type': 'VARCHAR(300) NOT NULL'}, 
    {'name': 'lv1_cat', 'type': 'VARCHAR(300)'}, 
    {'name': 'lv2_cat', 'type': 'VARCHAR(300)'}, 
    {'name': 'lv3_cat', 'type': 'VARCHAR(300)'}, 
    {'name': 'lv4_cat', 'type': 'VARCHAR(300)'}, 
    {'name': 'embeddings', 'type': 'TEXT'}, 
    {"name": "created_at", "type": "TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP"},
    {"name": "updated_at", "type": "TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP"}
    ]




if __name__ == "__main__":

    tab1, tab2, tab3, tab4, tab5 = st.tabs(["Make_RDB", "Upload", "Parsing", "Insert_Data", "ElasticSearch"])
    with tab1: 
        st.title("Create Postgres RDB")
        table_name = st.text_input("테이블명 입력", placeholder="예: my_table", value="test007")

        if st.button("🚀 테이블 생성 요청"):
            if table_name.strip() == "":
                st.error("테이블명을 입력하세요.")
            else:
                # FastAPI 요청 payload 생성
                payload = {
                    "table_name": table_name,
                    "columns": col_schema
                    }

                try:
                    res = requests.post(f"{FASTAPI_BASEURL}/create_tables", json=payload)

                    if res.status_code == 200:
                        st.success(res.json().get("message"))
                    else:
                        st.error(f"오류: {res.text}")

                except Exception as e:
                    st.error(f"API 호출 중 오류: {str(e)}")

        if st.button("🔍 테이블 생성 결과 확인"):
            try:
                res = requests.get(f"{FASTAPI_BASEURL}/tables")
                if res.status_code == 200:
                    tables = res.json().get("tables", [])

                    if table_name not in tables:
                        st.info("테이블이 없습니다.")
                    else:
                        st.info(f"테이블 {table_name}이 잘 생성되었습니다..")
                else:
                    st.error(f"오류: {res.text}")

            except Exception as e:
                st.error(f"API 호출 중 오류: {str(e)}")

    with tab2:
        st.title("Local File Upload Example")
        local_base_path = "C:\\Users\\jongb\\OneDrive\\바탕 화면\\temp"   # Local top folder path
        local_base_path = local_base_path.replace("\\", "/")
        server_top_path = "project01"                     # Server top folder path

        folder_path = st.text_input("로컬 파일 베이스 경로를 입력하세요(서버 저장 경로에서는 제거 대상)", value=local_base_path)
        server_top_path = st.text_input("서버에 저장할 최상위 폴더명(프로젝트명)을 입력하세요", value=server_top_path)

        if st.button("대용량 청킹 파일 전송"):
            if not os.path.exists(folder_path):
                st.error("❌ 경로가 존재하지 않습니다.")
                st.stop()

            total_files = count_files(folder_path)
            if total_files == 0:
                st.warning("📁 전송할 파일이 없습니다.")
                st.stop()

            progress_bar = st.progress(0)
            status = st.empty()

            files = list_files_recursive(folder_path)

            for idx, local_path in enumerate(files, start=1):
                upload_file_in_chunks(local_path=local_path, server_top_path=server_top_path)

                progress = idx / total_files
                progress_bar.progress(progress)
                status.write(f"({idx}/{total_files}) 업로드 중: {local_path}")

            st.success("🎉 모든 파일 업로드 완료!")

    with tab3:
        st.title("PDF Parsing 배치 처리")

        # 폴더 경로 입력
        folder_path = st.text_input("폴더 경로를 입력하세요", "./docs/uploaded")

        # remove_original 옵션
        remove_original = st.checkbox("처리 후 원본 파일 삭제(위 폴더 경로 내부 폴더 및 파일 삭제)", value=False)
        remove_original

        if st.button("배치 처리 시작"):
            if not folder_path:
                st.error("폴더 경로를 입력하세요.")
            else:
                with st.spinner("배치 처리 중..."):
                    try:
                        response = requests.post(
                            f"{FASTAPI_BASEURL}/batch_parse_by_folder",
                            data={
                                "folder_path": folder_path,
                                "remove_original": remove_original
                            }
                        )
                        if response.status_code == 200:
                            result = response.json()
                            st.success(f"배치 처리 완료! - 총 {len(result)}개 문서")
                        else:
                            st.error(f"에러 발생: {response.status_code} - {response.text}")
                    except Exception as e:
                        st.error(f"서버 연결 실패: {e}")
    
    with tab4:
        st.title("Postgres 데이터 Insert")

        # -----------------------------
        # 💠 1) 피클 파일에서 DB로 데이터 삽입
        # -----------------------------
        st.markdown("피클 파일에서 DB로 데이터 삽입")

        table_name = st.text_input("테이블 이름")
        pickle_folder = st.text_input("피클 폴더 경로")
        submitted = st.button("삽입 실행")
        with st.spinner("Processing..."):
            if submitted:
                try:
                    response = requests.post(
                        f"{FASTAPI_BASEURL}/insert_from_pickle",
                        data={"table_name": table_name, "pickle_path": pickle_folder}
                        )
                    if response.status_code == 200:
                        st.success(response.json().get("message"))
                    else:
                        st.error(response.json().get("detail", "알 수 없는 오류"))
                except Exception as e:
                    st.error(f"서버 요청 중 오류 발생: {e}")

    with tab5:
        with st.expander("1. 문서 색인 요청"):
            st.header("1. 문서 색인 요청")
            st.subheader("`/index/document` 엔드포인트")

            with st.form("index_form"):
                # 입력 필드
                table_name = st.text_input("**Table Name**", key="index_table_name", placeholder="예: my_documents_table")
                hashed_filepath = st.text_input("**Hashed Filepath (ID)**", key="index_hashed_filepath", placeholder="예: 0a1b2c3d4e5f6g7h")
                
                # 폼 제출 버튼
                submit_index = st.form_submit_button("🚀 문서 색인 요청")

                if submit_index:
                    if not table_name or not hashed_filepath:
                        st.error("⚠️ Table Name과 Hashed Filepath를 모두 입력해주세요.")
                    else:
                        endpoint_url = f"{FASTAPI_BASEURL}/index/document"
                        payload = {
                            "table_name": table_name,
                            "hashed_filepath": hashed_filepath
                        }
                        
                        st.info(f"요청 URL: **POST** `{endpoint_url}`")
                        st.json(payload)
                        
                        try:
                            # API 호출
                            response = requests.post(endpoint_url, json=payload, timeout=10)
                            
                            # 결과 처리
                            if response.status_code == 200:
                                st.success("✅ **색인 요청 성공!**")
                                st.json(response.json())
                            else:
                                st.error(f"❌ **색인 요청 실패!** (Status Code: {response.status_code})")
                                try:
                                    st.json(response.json())
                                except json.JSONDecodeError:
                                    st.text(response.text)
                                    
                        except requests.exceptions.ConnectionError:
                            st.error(f"🔌 **연결 오류:** API 서버 ({FASTAPI_BASEURL})에 연결할 수 없습니다. 서버가 실행 중인지 확인해주세요.")
                        except requests.exceptions.Timeout:
                            st.error("⏳ **시간 초과 오류:** API 응답 시간이 초과되었습니다.")
                        except Exception as e:
                            st.exception(e)
        with st.expander("2. 문서 조회 요청"):
            st.header("2. 문서 조회 요청")
            st.subheader("`/document/{hashed_filepath}` 엔드포인트")

            with st.form("get_form"):
                # 입력 필드
                hashed_filepath_get = st.text_input("**Hashed Filepath (ID)**", key="get_hashed_filepath", placeholder="예: 0a1b2c3d4e5f6g7h")
                
                # 폼 제출 버튼
                submit_get = st.form_submit_button("🔍 문서 조회")

            if submit_get:
                if not hashed_filepath_get:
                    st.error("⚠️ Hashed Filepath를 입력해주세요.")
                else:
                    endpoint_url = f"{FASTAPI_BASEURL}/document/{hashed_filepath_get}"
                    
                    st.info(f"요청 URL: **GET** `{endpoint_url}`")
                    
                    try:
                        # API 호출
                        response = requests.get(endpoint_url, timeout=10)
                        
                        # 결과 처리
                        if response.status_code == 200:
                            st.success(f"✅ **문서 조회 성공! - {len(response.json())}**")
                            st.json(response.json())
                        elif response.status_code == 404:
                            st.warning("⚠️ **문서를 찾을 수 없음** (Status Code: 404)")
                            st.json(response.json())
                        else:
                            st.error(f"❌ **문서 조회 실패!** (Status Code: {response.status_code})")
                            try:
                                st.json(response.json())
                            except json.JSONDecodeError:
                                st.text(response.text)
                                
                    except requests.exceptions.ConnectionError:
                        st.error(f"🔌 **연결 오류:** API 서버 ({FASTAPI_BASEURL})에 연결할 수 없습니다. 서버가 실행 중인지 확인해주세요.")
                    except requests.exceptions.Timeout:
                        st.error("⏳ **시간 초과 오류:** API 응답 시간이 초과되었습니다.")
                    except Exception as e:
                        st.exception(e)

        with st.expander("3. 문서 검색 요청 (하이브리드 지원)"):
            st.header("3. 문서 검색 요청 (하이브리드 지원)")
            st.subheader("`/search` 엔드포인트")

            with st.form("search_form"):
                # 입력 필드: 쿼리 텍스트
                query_text = st.text_area("**검색 쿼리 (query_text)**", key="search_query_text", height=100, placeholder="검색할 내용을 입력하세요. 예: 새로운 에너지 정책의 주요 내용")
                
                # 옵션 필드: size 및 min_score
                col1, col2 = st.columns(2)
                with col1:
                    size = st.number_input("**반환할 문서 개수 (size)**", min_value=1, max_value=50, value=5, step=1, key="search_size")
                with col2:
                    # 0.0을 포함한 실수 입력 가능
                    min_score = st.text_input("**최소 점수 (min_score)**", value="0.5", key="search_min_score")
                
                # 폼 제출 버튼
                submit_search = st.form_submit_button("🔍 문서 검색 실행")

            if submit_search:
                # min_score 입력값 유효성 검사 및 float 변환
                try:
                    min_score_float = float(min_score)
                except ValueError:
                    st.error("⚠️ 최소 점수(min_score)는 유효한 숫자로 입력해야 합니다.")
                    st.stop()
                    
                if not query_text:
                    st.error("⚠️ 검색 쿼리(query_text)를 입력해주세요. 이 필드는 필수입니다.")
                    # 백엔드 로직에 따라 query_embedding이 제공되면 query_text가 없어도 되지만,
                    # UI에서는 사용자 편의상 query_text 입력을 기본으로 유도합니다.
                else:
                    endpoint_url = f"{FASTAPI_BASEURL}/search"
                    payload = {
                        # UI는 query_text만 입력받고, query_embedding은 백엔드가 생성하도록 요청
                        "query_text": query_text,
                        "size": size,
                        "min_score": min_score_float
                    }
                    
                    st.info(f"요청 URL: **POST** `{endpoint_url}`")
                    st.json(payload)
                    
                    try:
                        # API 호출
                        response = requests.post(endpoint_url, json=payload, timeout=20) # 검색은 시간이 더 걸릴 수 있으므로 Timeout 증가
                        
                        # 결과 처리
                        if response.status_code == 200:
                            st.success("✅ **검색 요청 성공!**")
                            response_data = response.json()
                            st.markdown(f"**검색 유형:** `{response_data.get('query_type')}` | **총 결과 개수:** `{response_data.get('total_hits')}`")
                            st.json(response_data.get("results"))
                        else:
                            st.error(f"❌ **검색 요청 실패!** (Status Code: {response.status_code})")
                            try:
                                st.json(response.json())
                            except json.JSONDecodeError:
                                st.text(response.text)
                                
                    except requests.exceptions.ConnectionError:
                        st.error(f"🔌 **연결 오류:** API 서버 ({FASTAPI_BASEURL})에 연결할 수 없습니다. 서버가 실행 중인지 확인해주세요.")
                    except Exception as e:
                        st.exception(e)
            
                    