import os
import requests
import streamlit as st

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

if __name__ == "__main__":

    st.title("Local File Upload Example")
    local_base_path = "C:/Users/jongb/Desktop/temp"   # Local top folder path
    server_top_path = "project01"                     # Server top folder path

    folder_path = st.text_input("로컬 파일 경로를 입력하세요", value=local_base_path)

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
