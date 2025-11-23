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

FASTAPI_BASEURL = "http://localhost:8000"


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
    st.title(":blue[Auto VectorDB]")
    st.markdown("---")

    col1, col2, col3, col4, col5 = st.columns(5)
    with col1: 
        st.subheader(":blue[Create Postgres Table]")
        st.info("파싱 데이터를 저장할 RDB 준비")
        with st.expander("Create Table"):
            table_name = st.text_input("테이블명 입력(프로젝트명과 동일하게)", placeholder="예: my_table", value="프로젝트명")

            if st.button("🚀 테이블 생성"):
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

            if st.button("🔍 테이블 확인"):
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

    with col2:
        st.subheader(":green[Local File Upload]")
        st.info("로컬 파일을 서버 사이드로 이동")
        local_base_path_sample = "C:\\Users\\jongb\\OneDrive\\바탕 화면\\temp\\프로젝트명"   # Local top folder path
        local_base_path_sample = local_base_path_sample.replace("\\", "/")

        with st.expander("File Upload"):
            local_base_path = st.text_input("로컬 프로젝트 폴더 경로를 입력하세요", value=local_base_path_sample)
            local_base_path = local_base_path.replace("\\", "/")
        
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

    with col3:
        st.subheader(":blue[PDF Parsing 배치 처리]")
        st.info("PDF 파싱후 Pickle 형식 저장")

        with st.expander("Parsing with Docling"):

            # 폴더 경로 입력
            folder_path = st.text_input("폴더 경로를 입력하세요", "./docs/uploaded/프로젝트명")

            # remove_original 옵션
            remove_original = st.checkbox("처리 후 원본 파일 삭제(위 폴더 경로 내부 폴더 및 파일 삭제)", value=False)

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
    
    with col4:
        st.subheader(":green[Postgres 데이터 Insert]")
        st.info("Pickle 데이터를 RDB에 저장")

        with st.expander("Data Insert"):

            # -----------------------------
            # 💠 1) 피클 파일에서 DB로 데이터 삽입
            # -----------------------------
            table_name = st.text_input("테이블 이름", value="프로젝트명")
            pickle_folder = st.text_input("피클 폴더 경로", value="./docs/parsed/프로젝트명")
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


        with st.expander("결과 확인(Hashed FilePath 조회)"):

            table_name = st.text_input("Table Name", value="프로젝트명")
            if st.button("조회 실행"):
                if not table_name:
                    st.error("table_name과 hashed_filepath를 모두 입력하세요.")
                else:
                    with st.spinner("API 호출 중..."):
                        try:
                            url = f"{FASTAPI_BASEURL}/unique-filepath/{table_name}"
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

    with col5:
        st.subheader(":blue[Elastic Indexing]")
        st.info("RDB 데이터를 Elastic 인덱싱")
        with st.expander("1. 문서 색인"):

            st.session_state.hashed_filepath
            with st.form("index_form"):
                # 입력 필드
                table_name = st.text_input("**Table Name(=index_name)**", key="index_table_name", placeholder="예: 프로젝트명")
                # hashed_filepath = st.text_input("**Hashed Filepath (ID)**", key="index_hashed_filepath", placeholder="예: 0a1b2c3d4e5f6g7h")
                
                # 폼 제출 버튼
                submit_index = st.form_submit_button("🚀 문서 색인 요청")

                if submit_index:
                    for hashed_filepath in st.session_state.hashed_filepath:
                        if not table_name or not hashed_filepath:
                            st.error("⚠️ Table Name과 Hashed Filepath를 모두 입력해주세요.")
                        else:
                            endpoint_url = f"{FASTAPI_BASEURL}/index/document"
                            payload = {
                                "index_name": table_name,
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

        with st.expander("2. 문서 조회 테스트"):
            with st.form("get_form"):
                # 입력 필드
                index_name = st.text_input("**Index_Name**", key="ggg123", placeholder="프로젝트명")
                hashed_filepath_get = st.text_input("**Hashed Filepath (ID)**", key="get_hashed_filepath", placeholder="예: 0a1b2c3d4e5f6g7h")
                
                # 폼 제출 버튼
                submit_get = st.form_submit_button("🔍 문서 조회")

            if submit_get:
                if not hashed_filepath_get:
                    st.error("⚠️ Hashed Filepath를 입력해주세요.")
                else:
                    endpoint_url = f"{FASTAPI_BASEURL}/document/{index_name}/{hashed_filepath_get}"
                    
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

        with st.expander("3. 문서 검색 테스트"):
            with st.form("search_form"):
                # 입력 필드: 쿼리 텍스트
                index_name = st.text_input("**Index_Name**", key="index_name", placeholder="프로젝트명")
                query_text = st.text_area("**검색 쿼리 (query_text)**", key="search_query_text", height=100, placeholder="검색할 내용을 입력하세요")
                
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
                        "index_name": index_name,
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
            
                    