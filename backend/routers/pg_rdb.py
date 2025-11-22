import os
from pathlib import Path
from fastapi import APIRouter, HTTPException, UploadFile, File, Form, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Dict, Optional
from process.postgres import PostgresPipeline


from utils.config import get_config
from utils.schema import pg_schema
from utils.setlogger import setup_logger
config = get_config()
logger = setup_logger(f"{__name__}", level=config.LOG_LEVEL)


# PostgresPipeline 인스턴스 생성
pg = PostgresPipeline()
pg_api = APIRouter()


def list_files_recursive(folder_path: str):
    """폴더 안의 파일을 재귀적으로 읽어서 제너레이터로 반환하는 함수"""
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            yield os.path.join(root, file)




# -----------------------------
# 💠 1) 테이블명 조회
# -----------------------------
@pg_api.get("/tables", summary="모든 테이블 조회", tags=["Postgres"])
def get_all_tables():
    try:
        tables = pg.get_all_tables()
        return {"tables": tables}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------
# 💠 2) 테이블 생성
# -----------------------------
class ColumnConfig(BaseModel):
    name: str
    type: str



class CreateTableRequest(BaseModel):
    table_name: str
    columns: List[ColumnConfig] = pg_schema


@pg_api.post("/create_tables", summary="테이블 생성", tags=["Postgres"])
def create_table(data: CreateTableRequest):
    try:
        tables = pg.get_all_tables()
        if data.table_name not in tables:
            pg.create_table(
                table_name=data.table_name,
                columns_config=[col.model_dump() for col in data.columns]
                )
            logger.info(f"'{data.table_name}' 테이블 생성 완료")
            return {"message": f"'{data.table_name}' 테이블 생성 완료"}
        else:
            logger.warning(f"테이블 {data.table_name}는 이미 존재합니다.")
            return {"message": f"'{data.table_name}' 테이블이 이미 존재합니다."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------
# 💠 3) 테이블 삭제
# -----------------------------
@pg_api.delete("/tables/{table_name}", summary="테이블 삭제", tags=["Postgres"])
def delete_table(table_name: str):
    try:
        tables = pg.get_all_tables()
        if table_name in tables:
            pg.drop_table(table_name)
            logger.info(f"'{table_name}' 테이블 삭제 완료")
            return {"message": f"'{table_name}' 테이블 삭제 완료"}
        else:
            logger.warning(f"테이블 {table_name}는 존재하지 않습니다.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

# -----------------------------
# 💠 4) 데이터 추가
# -----------------------------
@pg_api.post("/insert_from_pickle", summary="피클 파일에서 DB로 데이터 삽입", tags=["Postgres"])
async def insert_from_pickle(
    table_name: str = Form(...),
    pickle_path: str = Form(...)
    ):
    """
    서버 내 pickle 파일 경로를 받아 데이터를 DB에 insert
    """
    try:
        # 실제 삽입 처리
        files = list_files_recursive(pickle_path)
        for pickle_path in files:
            pickle_path = pickle_path.replace("\\", "/")
            if pickle_path.endswith(".pkl"):
                pg.insert_data_from_pickle(table_name, pickle_path)     

        return {"message": f"Data inserted successfully from {pickle_path}"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# ----------------------------- 
# 💠 5) 데이터 조회
# -----------------------------

@pg_api.get("/select_all", summary="테이블 데이터 조회", tags=["Postgres"])
async def select_all(
    table_name: str = Query(..., description="조회할 테이블명"),
    limit: Optional[int] = Query(10, description="조회할 데이터 수 제한"),
    order_by: str = Query("id", description="정렬할 컬럼명")
    ):
    """
    지정된 테이블에서 데이터를 조회합니다.
    """
    try:
        # 데이터 조회
        results = pg.select_all_data(table_name=table_name, limit=limit, order_by=order_by)

        if not results:
            return {"message": "데이터가 없습니다", "data": []}

        return {"message": "Success", "data": results}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))