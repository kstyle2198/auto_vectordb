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
    save_path = f"./uploaded/{server_path}/{file.filename}"
    os.makedirs(f"./uploaded/{server_path}", exist_ok=True)

    with open(save_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    logger.info({"message": "파일 업로드 성공","original_path": local_path,"saved_path": save_path,"filename": file.filename,})
    return {"message": "파일 업로드 성공","original_path": local_path,"saved_path": save_path,"filename": file.filename,}


@upload_api.post("/upload_chunk", tags=["Upload"])
async def upload_chunk(
    file: UploadFile = File(...),
    filename: str = Form(...),
    chunk_index: int = Form(...),
    total_chunks: int = Form(...),
    server_path: str = Form(...),
    ):
    """
    청크 단위 파일 업로드 API
    
    대용량 파일을 청크(chunk)로 분할하여 순차적으로 업로드합니다.
    각 청크는 서버에서 임시 저장되며, 모든 청크 업로드 완료 시 최종 파일이 생성됩니다.
    
    Args:
        file (UploadFile): 업로드할 파일 청크 데이터
        filename (str): 최종 완성될 파일명
        chunk_index (int): 현재 업로드 중인 청크의 인덱스 (0부터 시작)
        total_chunks (int): 전체 청크 개수
        server_path (str): 서버에 저장할 상대 경로 (uploaded/ 디렉토리 하위)
    
    Returns:
        dict: 청크 업로드 결과 정보
            - message (str): 처리 결과 메시지
            - saved_path (str): 서버에 저장된 전체 경로 (마지막 청크에서만 반환)
            - filename (str): 업로드된 파일명 (마지막 청크에서만 반환)
    
    Raises:
        HTTPException: 파일 저장 중 오류 발생 시 422 에러 반환
    
    Notes:
        - 각 청크는 append 모드로 저장되어 최종 파일을 구성합니다
        - chunk_index는 0-based 인덱스입니다 (0, 1, 2, ...)
        - 마지막 청크 업로드 시 완료 메시지와 함께 저장 경로 정보를 반환합니다
        - 중간 청크 업로드 시 진행 상태 메시지만 반환합니다
    """
    try:
        # 최종 저장될 파일 경로
        folder = f"./uploaded/{server_path}"
        os.makedirs(folder, exist_ok=True)
        save_path = f"{folder}/{filename}"
        # 각 chunk를 append 형식으로 저장
        async with aiofiles.open(save_path, "wb") as f:
            content = await file.read()
            await f.write(content)

        # 마지막 chunk라면 로그 출력
        if chunk_index + 1 == total_chunks:
            return {
                "message": "업로드 완료",
                "saved_path": save_path,
                "filename": file.filename,
                }
        
        logger.info({"message": f"chunk {chunk_index + 1}/{total_chunks} 업로드 완료",})
        return {"message": f"chunk {chunk_index + 1}/{total_chunks} 업로드 완료",}

    except Exception as e:
        logger.error(f"업로드 실패: {str(e)}")
        raise HTTPException(status_code=422, detail=str(e))