import os
from pathlib import Path
from fastapi import APIRouter, HTTPException, UploadFile, File, Form, Query
from pydantic import BaseModel
from typing import List, Optional
from process.maria import MariaPipeline  # MariaPipeline 임포트

from utils.config import get_config
from utils.schema import maria_schema
from utils.setlogger import setup_logger

config = get_config()
logger = setup_logger(f"{__name__}", level=config.LOG_LEVEL)

# MariaPipeline 인스턴스 생성
maria = MariaPipeline()
maria_api = APIRouter()


# -----------------------------
# 📂 유틸 함수
# -----------------------------
def list_files_recursive(folder_path: str):
    """폴더 안의 파일을 재귀적으로 읽어서 제너레이터로 반환"""
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            yield os.path.join(root, file)


# -----------------------------
# 💠 1) 테이블명 조회
# -----------------------------
@maria_api.get("/mr_tables", summary="모든 테이블 조회", tags=["MariaDB"])
def get_all_tables():
    try:
        tables = maria.get_all_tables()
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
    columns: List[ColumnConfig] = maria_schema


@maria_api.post("/mr_create_table", summary="테이블 생성", tags=["MariaDB"])
def create_table(data: CreateTableRequest):
    try:
        tables = maria.get_all_tables()
        if data.table_name in tables:
            return {"message": f"'{data.table_name}' 테이블이 이미 존재합니다."}

        maria.create_table(
            table_name=data.table_name,
            columns_config=[col.model_dump() for col in data.columns]
        )
        return {"message": f"'{data.table_name}' 테이블 생성 완료"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------
# 💠 3) 테이블 삭제
# -----------------------------
@maria_api.delete("/mr_tables/{table_name}", summary="테이블 삭제", tags=["MariaDB"])
def delete_table(table_name: str):
    try:
        tables = maria.get_all_tables()
        if table_name not in tables:
            return {"message": f"'{table_name}' 테이블이 존재하지 않습니다."}

        maria.drop_table(table_name)
        return {"message": f"'{table_name}' 테이블 삭제 완료"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------
# 💠 4) 데이터 삽입 (Pickle)
# -----------------------------
@maria_api.post("/mr_insert_from_pickle", summary="Pickle 데이터 삽입", tags=["MariaDB"])
async def insert_from_pickle(
    table_name: str = Form(...),
    pickle_path: str = Form(...)
):
    try:
        files = list_files_recursive(pickle_path)
        inserted_files = []

        for file_path in files:
            file_path = file_path.replace("\\", "/")
            if file_path.endswith(".pkl"):
                maria.insert_data_from_pickle(table_name, file_path)
                inserted_files.append(file_path)

        if not inserted_files:
            return {"message": "Pickle 파일을 찾지 못했습니다."}

        return {"message": f"Data inserted successfully from {len(inserted_files)} file(s)"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------
# 💠 5) 데이터 조회
# -----------------------------
@maria_api.get("/mr_select_all", summary="테이블 데이터 조회", tags=["MariaDB"])
async def select_all(
    table_name: str = Query(..., description="조회할 테이블명"),
    limit: Optional[int] = Query(10, description="조회할 데이터 수 제한"),
    order_by: str = Query("id", description="정렬할 컬럼명")
):
    try:
        results = maria.select_all_data(table_name=table_name, limit=limit, order_by=order_by)
        return {"message": "Success", "data": results} if results else {"message": "데이터가 없습니다", "data": []}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
