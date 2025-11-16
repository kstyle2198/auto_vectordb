import os
import requests
import streamlit as st
from pathlib import Path

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

    tab1, tab2, tab3, tab4 = st.tabs(["Make_RDB", "Upload", "Parsing", "Insert_RDB"])
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
        local_base_path = "C:/Users/jongb/Desktop/temp"   # Local top folder path
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
        folder_path = st.text_input("폴더 경로를 입력하세요", "./uploaded")

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
        st.title("Postgres 데이터 관리 UI")

        # -----------------------------
        # 💠 1) 피클 파일에서 DB로 데이터 삽입
        # -----------------------------
        st.header("피클 파일에서 DB로 데이터 삽입")

        with st.form("insert_from_pickle_form"):
            table_name = st.text_input("테이블 이름")
            pickle_path = st.text_input("피클 파일 경로")
            submitted = st.form_submit_button("삽입 실행")
            with st.spinner("Processing..."):
                if submitted:
                    if not table_name or not pickle_path:
                        st.error("테이블 이름과 피클 파일 경로를 입력해주세요.")
                    else:
                        try:
                            response = requests.post(
                                f"{FASTAPI_BASEURL}/insert_from_pickle",
                                data={"table_name": table_name, "pickle_path": pickle_path}
                            )
                            if response.status_code == 200:
                                st.success(response.json().get("message"))
                            else:
                                st.error(response.json().get("detail", "알 수 없는 오류"))
                        except Exception as e:
                            st.error(f"서버 요청 중 오류 발생: {e}")

        # -----------------------------
        # 💠 2) 테이블 데이터 조회
        # -----------------------------
        st.header("테이블 데이터 조회")

        with st.form("select_all_form"):
            table_name_query = st.text_input("조회할 테이블 이름")
            limit = st.number_input("조회 수 제한", min_value=1, max_value=1000, value=10)
            order_by = st.text_input("정렬할 컬럼명", value="id")
            submitted_query = st.form_submit_button("조회 실행")

            if submitted_query:
                if not table_name_query:
                    st.error("테이블 이름을 입력해주세요.")
                else:
                    try:
                        response = requests.get(
                            f"{FASTAPI_BASEURL}/select_all",
                            params={
                                "table_name": table_name_query,
                                "limit": limit,
                                "order_by": order_by
                            }
                        )
                        if response.status_code == 200:
                            data = response.json().get("data", [])
                            if data:
                                st.dataframe(data)
                            else:
                                st.info("조회 결과가 없습니다.")
                        else:
                            st.error(response.json().get("detail", "알 수 없는 오류"))
                    except Exception as e:
                        st.error(f"서버 요청 중 오류 발생: {e}")


                