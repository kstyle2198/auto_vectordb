from utils.config import get_config
from utils.setlogger import setup_logger
config = get_config()
logger = setup_logger(f"{__name__}", level=config.LOG_LEVEL)

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware


from core.lifespan import lifespan
# ---------------------------------------------------
# FastAPI 앱
# ---------------------------------------------------
app = FastAPI(title="Auto VectorDB API",  version="0.1.1", description="벡터DB 자동화 파이프라인 API", lifespan=lifespan,)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
    )

from routers.upload import upload_api
app.include_router(upload_api)

from routers.parser import parser_api
app.include_router(parser_api)

from celery.result import AsyncResult
from routers.task_router import long_running_task
from pydantic import BaseModel
class ParsingRequest(BaseModel):
    pdf_path: str

@app.post("/background_parsing", tags=["Parser"])
async def background_parsing(request: ParsingRequest):
    result = long_running_task.delay(request.pdf_path)
    return {
        "message": f"Parsing {request.pdf_path} has been queued.",
        "task_id": result.id,
        "status": "queued"
        }

# ⭐️ 폴링 엔드포인트 추가
@app.get("/task_status/{task_id}", tags=["Parser"])
async def get_task_status(task_id: str):
    """
    Celery task_id로 상태와 결과를 조회하는 API
    """
    result = AsyncResult(task_id)

    return {
        "task_id": task_id,
        "status": result.status,     # PENDING / STARTED / RETRY / FAILURE / SUCCESS
        "result": result.result if result.successful() else None
    }


from routers.pg_rdb import pg_api
app.include_router(pg_api)

from routers.es_index import es_api
app.include_router(es_api)


if __name__ == "__main__":
    import uvicorn
    logger.info("Starting FastAPI server...")
    uvicorn.run("server:app", host="0.0.0.0", port=8001, reload=True, workers=1)