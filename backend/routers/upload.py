import os
import shutil
import aiofiles
from fastapi import APIRouter, HTTPException, UploadFile, File, Form, HTTPException

from utils.config import get_config
from utils.setlogger import setup_logger
config = get_config()
logger = setup_logger(f"{__name__}", level=config.LOG_LEVEL)

upload_api = APIRouter()

@upload_api.post("/upload", tags=["Upload"])
async def upload(file: UploadFile = File(...), local_path: str = Form(...), server_path: str = Form(...)):
    """
    파일 업로드 API
    
    클라이언트에서 파일을 서버의 지정된 경로에 업로드합니다.
    
    Args:
        file (UploadFile): 업로드할 파일 객체
        local_path (str): 클라이언트 측 원본 파일 경로 (로그용)
        server_path (str): 서버에 저장할 상대 경로 (uploaded/ 디렉토리 하위)
    
    Returns:
        dict: 업로드 결과 정보
            - message (str): 처리 결과 메시지
            - original_path (str): 원본 파일 경로
            - saved_path (str): 서버에 저장된 전체 경로
            - filename (str): 업로드된 파일명
    
    Raises:
        HTTPException: 파일 저장 중 오류 발생 시 500 에러 반환
    """
    save_dir = os.path.join("./docs/uploaded", server_path)
    os.makedirs(save_dir, exist_ok=True)
    save_path = os.path.join(save_dir, file.filename)

    with open(save_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    logger.info({"message": "파일 업로드 성공","original_path": local_path,"saved_path": save_path,"filename": file.filename,})
    return {"message": "파일 업로드 성공","original_path": local_path,"saved_path": save_path,"filename": file.filename,}
