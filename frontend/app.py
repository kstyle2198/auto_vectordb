# streamlit run app.py --server.port 8502

import os
import json
import base64
import requests
import streamlit as st
import streamlit.components.v1 as components
from pathlib import Path
from tqdm.auto import tqdm

st.set_page_config(page_title="Auto VectorDB", page_icon="🐬", layout="wide", initial_sidebar_state="collapsed")

from utils.config import get_config
from utils.setlogger import setup_logger
config = get_config()
logger = setup_logger(f"{__name__}", level=config.LOG_LEVEL)

FASTAPI_BASEURL = config.FASTAPI_BASEURL
SCHEMA_NAME = config.SCHEMA_NAME


from utils.style import HOVERING_EFFECT
# ==== Background Image ====
def get_base64_of_image(image_file):
    """이미지 파일을 Base64로 인코딩하여 문자열로 반환합니다."""
    with open(image_file, "rb") as f:
        data = f.read()
    return base64.b64encode(data).decode()

def set_background(image_file, overlay_color="rgba(255,255,255,0.5)"):
    """
    CSS를 사용하여 부드럽게 움직이는 배경 이미지와 오버레이를 설정합니다.
    """
    bin_str = get_base64_of_image(image_file)
    page_bg_img = f"""
    <style>
    /* 움직이는 애니메이션 효과 정의 */
    @keyframes panImage {{
        0%   {{ background-position: 0% 50%; }}
        50%  {{ background-position: 100% 50%; }}
        100% {{ background-position: 0% 50%; }}
    }}

    /* 앱 전체 배경 설정 */
    [data-testid="stAppViewContainer"],
    [data-testid="stHeader"] {{
        position: relative;
        background: url("data:image/png;base64,{bin_str}") no-repeat center center fixed;
        /* 이미지를 화면보다 약간만 크게 만들어 자연스러운 움직임 유도 */
        background-size: 115% auto;
        /* ⭐️ 개선된 부분: 지속시간, 타이밍 함수, 반복 */
        animation: panImage 80s ease-in-out infinite;
    }}

    /* 배경 위 오버레이 효과 */
    [data-testid="stAppViewContainer"]::before,
    [data-testid="stHeader"]::before {{
        content: "";
        position: absolute;
        top: 0; right: 0; bottom: 0; left: 0;
        background: {overlay_color};
        z-index: 0; /* 콘텐츠 뒤에 위치 */
    }}

    /* 콘텐츠가 오버레이 위에 오도록 설정 및 **글자색 검정으로 변경** */
    .stApp, [data-testid="stAppViewContainer"] {{
        position: relative;
        z-index: 1;
        color: black; /* 기본 글자색을 검정으로 설정 (추가된 부분) */**
    }}
    </style>
    """
    st.markdown(page_bg_img, unsafe_allow_html=True)

# --- 이미지 파일 경로 설정 (사용자 환경에 맞게 수정해주세요) ---
image_path = "./system_image/bg_img1.jpg"
if os.path.exists(image_path):
    # 오버레이 색상을 밝게 설정했으므로 글자색을 검정으로 변경하는 것이 가독성에 좋습니다.
    set_background(image_path, overlay_color="rgba(255,255,255,0.6)")
else:
    st.warning(f"배경 이미지 파일을 찾을 수 없습니다: {image_path}")

# Inject CSS style for Hover effect
st.markdown(HOVERING_EFFECT, unsafe_allow_html=True)

def make_hover_container(title:str, content:str, url:str, height:str = "auto"):
    st.markdown(f"""
            <a href="{url}" target="_blank" class="clickable-box-wrapper">
            <div class="hover-box" style="height: {height};">
                <h1>{title}</h1>
                <p>{content}</p></div>
            </a>
        """, unsafe_allow_html=True)
    
image_paths = [
    "./system_image/img1.jpg",
    "./system_image/img2.jpg",
    "./system_image/img3.jpg",
    "./system_image/img4.jpg",
]
# base64로 인코딩된 이미지 태그 생성 함수
def get_base64_img_tag(file_path):
    with open(file_path, "rb") as img_file:
        encoded = base64.b64encode(img_file.read()).decode()
        return f'<img src="data:image/png;base64,{encoded}" style="width: 100%; position: absolute; opacity: 0; transition: opacity 1s;">'

# 이미지 태그 리스트 생성
image_tags = ''.join([get_base64_img_tag(path) for path in image_paths])

# HTML + JS 코드로 슬라이드쇼 구성
html_code = f"""
<div id="slideshow" style="position: relative; width: 100%; max-width: 800px; margin: auto; height: 500px;">
  {image_tags}
</div>

<script>
const slides = document.querySelectorAll("#slideshow img");
let current = 0;

function showNextSlide() {{
    slides[current].style.opacity = 0;
    current = (current + 1) % slides.length;
    slides[current].style.opacity = 1;
}}

slides[0].style.opacity = 1;
setInterval(showNextSlide, 3000);
</script>
"""


if "hashed_filepath" not in st.session_state: st.session_state.hashed_filepath=[]

def list_files_recursive(folder_path: str):

    if not os.path.exists(folder_path):
        raise FileNotFoundError(
            f"폴더 없음: {folder_path}"
        )

    for root, dirs, files in os.walk(folder_path):

        for file in files:

            if file.endswith((".pickle", ".pkl")):

                yield os.path.join(
                    root,
                    file
                ).replace("\\", "/")

def count_files(folder_path: str) -> int:
    """총 파일 개수 (메모리 부담 없음)"""
    count = 0
    for _, _, files in os.walk(folder_path):
        count += len(files)
    return count

def upload_file_in_chunks(local_base_path:str, local_path:str):
    chunk_size = 10 * 1024 * 1024   # 10MB
    local_filename = os.path.basename(local_path)
    file_size = os.path.getsize(local_path)
    total_chunks = (file_size + chunk_size - 1) // chunk_size

    folder_path = os.path.dirname(local_path).replace("\\", "/") # 맨 끝 파일명 제외한 상위 경로
    delted_path = os.path.dirname(local_base_path).replace("\\", "/") # 폴더 경로에서 맨끝 폴더 제외 --> 서버 저장시 제거할 경로명
    server_path = f"{folder_path.replace('\\', '/').replace(delted_path, '')}"

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
    {'name': 'hashed_content', 'type': 'VARCHAR(64) UNIQUE NOT NULL'}, 
    {'name': 'page_content', 'type': 'TEXT NOT NULL'}, 
    {'name': 'metadata', 'type': 'JSONB'}, 
    {'name': 'dense_embeddings', 'type': 'VECTOR(1024)'}, 
    {'name': 'sparse_embeddings', 'type': 'JSONB'}, 
    {"name": "created_at", "type": "TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP"},
    {"name": "updated_at", "type": "TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP"}
    ]

if "project_name" not in st.session_state: st.session_state.project_name = ""
if "task_ids" not in st.session_state: st.session_state.task_ids = []
if "pending_results" not in st.session_state: st.session_state.pending_results = []
if "success_results" not in st.session_state: st.session_state.success_results = []

if __name__ == "__main__":

    st.session_state
    st.title(":blue[Auto VectorDB]")
    st.page_link(label="FastAPI Docs", page="http://localhost:8001/docs")
    st.markdown("---")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.subheader(":green[File Upload & Parsing]")
        st.info("로컬 파일을 서버 사이드로 이동")
        local_base_path_sample = "C:\\Users\\jongb\\OneDrive\\바탕 화면\\temp\\프로젝트명"   # Local top folder path
        local_base_path_sample = local_base_path_sample.replace("\\", "/")

        with st.expander("File Upload"):
            local_base_path = st.text_input("로컬 프로젝트 폴더 경로를 입력하세요", value=local_base_path_sample)
            local_base_path = local_base_path.replace("\\", "/")
            st.session_state.project_name = local_base_path.split("/")[-1]   # 맨 마지막 폴더명이 프로젝트명
        
            if st.button("대용량 청킹 파일 전송"):
                if not os.path.exists(local_base_path):
                    st.error("❌ 경로가 존재하지 않습니다.")
                    st.stop()

                total_files = count_files(local_base_path)
                if total_files == 0:
                    st.warning("📁 전송할 파일이 없습니다.")
                    st.stop()

                progress_bar = st.progress(0)
                status = st.empty()

                files = list_files_recursive(local_base_path)

                for idx, local_path in enumerate(files, start=1):
                    upload_file_in_chunks(local_base_path=local_base_path, local_path=local_path)

                    progress = idx / total_files
                    progress_bar.progress(progress)
                    status.write(f"({idx}/{total_files}) 업로드 중: {local_path}")

                st.success("🎉 모든 파일 업로드 완료!")

        with st.expander("Background Parsing"):

            folder_path = st.text_input("폴더 경로를 입력하세요", f"./docs/uploaded/{st.session_state.project_name}", key="wererww")

            if st.button("Task ID 초기화"):
                st.session_state.task_ids = []
                st.session_state.pending_results = []
                st.session_state.success_results = []

            if st.button("Background 처리 시작"):
                
                for file in requests.get(f"{FASTAPI_BASEURL}/list-files-stream", params={"folder_path": folder_path}, stream=True):
                    file = file.decode("utf-8")
                    file = json.loads(file)
                    st.info(f"target_file: {file}")
                    pdf_path = file["pdf_path"]
                    response = requests.post(f"{FASTAPI_BASEURL}/background_parsing", json = {"pdf_path": pdf_path})
                    data = response.json()
                    logger.info(data)
                    task_id = data.get("task_id")
                    st.session_state.task_ids.append(task_id)
                
                st.info("All Parsing Tasks are Queued")

            if st.button("Polling"):
                st.session_state.pending_results = []
                st.session_state.success_results = []
                for id in st.session_state.task_ids:
                    status_response = requests.get(f"{FASTAPI_BASEURL}/task_status/{id}")
                    status_data = json.loads(status_response.text)
                    if status_data not in st.session_state.pending_results and status_data["status"]!="SUCCESS" :
                        st.session_state.pending_results.append(status_data)
                    if status_data not in st.session_state.success_results and status_data["status"]=="SUCCESS" :
                        st.session_state.success_results.append(status_data)

            col111, col222 = st.columns(2)
            with col111:
                st.info(f"대기중인 작업: {len(st.session_state.pending_results)}")
                with st.container(border=True, height=500):
                    for p in st.session_state.pending_results:
                        st.warning(p)
            with col222:
                st.info(f"성공한 작업: {len(st.session_state.success_results)}")
                with st.container(border=True, height=500):
                    for s in st.session_state.success_results:
                        st.success(s)

    with col2:
        st.subheader(":green[Postgres 데이터 저장]")
        st.info("Pickle 데이터를 RDB에 저장")

        with st.expander("Create Table"):
            table_name = st.text_input("테이블명 입력(프로젝트명과 동일하게)", placeholder="예: my_table", value="프로젝트명")

            if st.button("🚀 테이블 생성"):
                if table_name.strip() == "":
                    st.error("테이블명을 입력하세요.")
                else:
                    # FastAPI 요청 payload 생성
                    payload = {"table_name": table_name, "columns": col_schema}

                    try:
                        res = requests.post(f"{FASTAPI_BASEURL}/create_tables", json=payload)

                        if res.status_code == 200:
                            st.success(res.json().get("message"))
                        else:
                            st.error(f"오류: {res.text}")

                    except Exception as e:
                        st.error(f"API 호출 중 오류: {str(e)}")

            if st.button("🔍 테이블 확인"):
                try:
                    res = requests.get(f"{FASTAPI_BASEURL}/tables")
                    if res.status_code == 200:
                        tables = res.json().get("tables", [])

                        if f"{SCHEMA_NAME}.{table_name}" not in tables:
                            st.info(f"{SCHEMA_NAME}.{table_name} 테이블이 없습니다.")
                        else:
                            st.info(f"테이블 {SCHEMA_NAME}.{table_name}이 잘 생성되었습니다..")
                    else:
                        st.error(f"오류: {res.text}")

                except Exception as e:
                    st.error(f"API 호출 중 오류: {str(e)}")

        with st.expander("Data Insert"):

            # -----------------------------
            # 💠 1) 피클 파일에서 DB로 데이터 삽입
            # -----------------------------
            table_name = st.text_input("테이블 이름", value="프로젝트명")
            project_name = st.text_input("프로젝트명", value="PDF 저장 최하위 폴더명")
            pickle_folder = f"{config.PICKLE_ABS_PATH}{project_name}"
            submitted = st.button("삽입 실행")
            if submitted:
                with st.spinner("Processing..."):
                    try:
                        response = requests.post(
                            f"{FASTAPI_BASEURL}/insert_from_pickle",
                            data={"table_name": table_name, "pickle_folder": pickle_folder}
                            )
                        if response.status_code == 200:
                            st.success(response.json().get("message"))
                        else:
                            st.error(response.json().get("detail", "알 수 없는 오류"))
                    except Exception as e:
                        st.error(f"서버 요청 중 오류 발생: {e}")

        with st.expander("결과 확인(Hashed FileContent 조회)"):

            table_name = st.text_input("Table Name", value="프로젝트명")
            if st.button("조회 실행"):
                if not table_name:
                    st.error("table_name과 hashed_file_content를 모두 입력하세요.")
                else:
                    with st.spinner("API 호출 중..."):
                        try:
                            url = f"{FASTAPI_BASEURL}/unique-hashed-content/{table_name}"
                            response = requests.get(url)

                            if response.status_code != 200:
                                st.error(f"❌ 서버 오류: {response.status_code}")
                            else:
                                data = response.json()

                                if data.get("status") == "ok":
                                    st.success("조회 성공!")
                                    st.write(f"총 개수: **{data.get('count')}**")
                                    st.session_state.hashed_filepath = data.get("hashed_filepaths")
                                    # st.json(data.get("hashed_filepaths"))
                                else:
                                    st.error(f"⚠️ 오류: {data.get('message')}")
                        except Exception as e:
                            st.error(f"API 호출 중 오류 발생: {e}")
            st.session_state.hashed_filepath

    with col3:
        st.subheader(":blue[ElasticSearch Indexing]")
        st.info("RDB 데이터를 Elastic 인덱싱")

        with st.expander("1.인덱스 생성"):
            with st.form("index-form"):
                index_name = st.text_input("**Index Name**", key="index_name_01", placeholder="예: 프로젝트명")
                payload = {"index_name": index_name,}

                st_create_index = st.form_submit_button("🚀 인덱스 생성 요청")
                if st_create_index:
                    try:
                        res = requests.post(f"{FASTAPI_BASEURL}/es/indices", json=payload)
                        if res.status_code == 200:
                            st.success(res.json())
                        else:
                            st.error(f"오류: {res.text}")

                    except Exception as e:
                        st.error(f"API 호출 중 오류: {str(e)}")

        with st.expander("2. 문서 색인"):
            with st.form("index_form"):
                # 입력 필드
                table_name = st.text_input("**Table Name(=index_name)**", key="index_table_name", placeholder="예: 프로젝트명")
                
                # 폼 제출 버튼
                submit_index = st.form_submit_button("🚀 문서 색인 요청")

                if submit_index:
                    endpoint_url = f"{FASTAPI_BASEURL}/es/bulk-index"
                    payload = {
                        "schema_table_name": f"{SCHEMA_NAME}.{table_name}",
                        "index_name": table_name,
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

        with st.expander("3. 문서 검색 테스트"):
            with st.form("search_form"):
                # 입력 필드: 쿼리 텍스트
                index_name = st.text_input("**Index_Name**", key="index_name", placeholder="프로젝트명")
                query_text = st.text_area("**검색 쿼리 (query_text)**", key="search_query_text", height=100, placeholder="검색할 내용을 입력하세요")
                
                # 옵션 필드: size
                col1, col2 = st.columns(2)
                with col1:
                    size = st.number_input("**반환할 문서 개수 (size)**", min_value=1, max_value=50, value=5, step=1, key="search_size")
                with col2:
                    pass
                
                # 폼 제출 버튼
                submit_search = st.form_submit_button("🔍 문서 검색 실행")

            if submit_search:
                    
                if not query_text:
                    st.error("⚠️ 검색 쿼리(query_text)를 입력해주세요. 이 필드는 필수입니다.")

                else:
                    endpoint_url = f"{FASTAPI_BASEURL}/es/hybrid-search"
                    payload = {
                        "index_name": index_name,
                        "query": query_text,
                        "size": int(size),
                    }
                    try:
                        # API 호출
                        response = requests.post(endpoint_url, json=payload, timeout=20) 
                        
                        # 결과 처리
                        if response.status_code == 200:
                            response_data = response.json()
                            st.success(f"✅ **검색 요청 성공!** - {len(response_data.get("hits"))}개")
                            with st.container(height=300, border=True):
                                st.info(response_data)
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
            
                    