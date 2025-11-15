from utils.config import get_config
from utils.setlogger import setup_logger
config = get_config()
logger = setup_logger(f"{__name__}", level=config.LOG_LEVEL)

import os
import shutil
import aiofiles
from fastapi import FastAPI, UploadFile, File, Form, HTTPException


app = FastAPI()

@app.post("/upload")
async def upload(file: UploadFile = File(...), local_path: str = Form(...), server_path: str = Form(...)):
    save_path = f"./uploaded/{server_path}/{file.filename}"
    os.makedirs(f"./uploaded/{server_path}", exist_ok=True)

    with open(save_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    logger.info({"message": "파일 업로드 성공","original_path": local_path,"saved_path": save_path,"filename": file.filename,})
    return {"message": "파일 업로드 성공","original_path": local_path,"saved_path": save_path,"filename": file.filename,}


@app.post("/upload_chunk")
async def upload_chunk(
    file: UploadFile = File(...),
    filename: str = Form(...),
    chunk_index: int = Form(...),
    total_chunks: int = Form(...),
    server_path: str = Form(...),
    ):
    try:
        # 최종 저장될 파일 경로
        folder = f"./uploaded/{server_path}"
        os.makedirs(folder, exist_ok=True)
        save_path = f"{folder}/{filename}"
        print(save_path)
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

if __name__ == "__main__":
    import uvicorn
    logger.info("Starting FastAPI server...")
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True, workers=2)