import os
from pathlib import Path
from fastapi import APIRouter, HTTPException, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse
from typing import Optional
from process.parsing import DoclingParser

from utils.config import get_config
from utils.setlogger import setup_logger
config = get_config()
logger = setup_logger(f"{__name__}", level=config.LOG_LEVEL)


# DoclingParser 인스턴스 생성
parser = DoclingParser(output_base_path="./docs")
parser_api = APIRouter()

@parser_api.post("/parse_pdf_by_path", tags=["Parser"])
async def parse_pdf_by_path(
    pdf_path: str = Form(...),
    lv1_cat: str = Form(...),
    lv2_cat: str = Form(...),   
    lv3_cat: Optional[str] = Form(""),
    lv4_cat: Optional[str] = Form(""),
    remove_original: bool = Form(False)
    ):
    """
    이미 서버에 저장된 PDF 경로를 받아 페이지별 파싱
    """
    path_obj = Path(pdf_path)
    if not path_obj.exists() or not path_obj.is_file():
        raise HTTPException(status_code=400, detail=f"PDF 파일을 찾을 수 없습니다: {pdf_path}")

    try:
        docs = parser.parse_pdf_by_page(
            pdf_path=str(path_obj),
            lv1_cat=lv1_cat,
            lv2_cat=lv2_cat,
            lv3_cat=lv3_cat,
            lv4_cat=lv4_cat,
            remove_original=remove_original
        )

        # Document 객체를 JSON 직렬화
        result = [
            {"page_content": doc.page_content, "metadata": doc.metadata} for doc in docs
        ]
        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@parser_api.post("/batch_parse_by_folder", tags=["Parser"])
async def batch_parse_by_folder(
    folder_path: str = Form(...),
    remove_original: bool = Form(False)
    ):
    """
    이미 저장된 폴더 내 PDF를 모두 배치 처리
    """
    folder = Path(folder_path)
    if not folder.exists() or not folder.is_dir():
        raise HTTPException(status_code=400, detail=f"폴더를 찾을 수 없습니다: {folder_path}")

    try:
        all_docs = parser.batch_parse_pdfs(folder_path=str(folder), remove_original=remove_original)
        result = []
        for doc_list in all_docs:
            docs = []
            for doc in doc_list:
                # metadata 내 Path 타입 변환
                meta = {
                    key: str(value) if isinstance(value, Path) else value
                    for key, value in doc.metadata.items()
                    }
                docs.append({
                    "page_content": doc.page_content,
                    "metadata": meta
                })
            result.append(docs)

        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))