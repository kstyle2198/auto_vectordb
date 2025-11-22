import re
import json
import yaml
import shutil
from pathlib import Path
import os
import time
import hashlib
from uuid import uuid4
import pickle
import pdfplumber
from tqdm.auto import tqdm
from langchain_core.documents import Document
from typing import List, Optional
from langchain_ollama import OllamaEmbeddings

from docling.backend.pypdfium2_backend import PyPdfiumDocumentBackend
from docling.datamodel.base_models import InputFormat
from docling.document_converter import (
    DocumentConverter,
    PdfFormatOption,
    WordFormatOption,
    )
from docling.datamodel.pipeline_options import PdfPipelineOptions, EasyOcrOptions
from docling.pipeline.simple_pipeline import SimplePipeline
from docling.pipeline.standard_pdf_pipeline import StandardPdfPipeline

from collections import defaultdict

class DoclingParser:
    """PDF 문서를 Docling을 사용하여 파싱하는 클래스"""
    
    # 클래스 상수
    NEWLINE_PATTERN = re.compile(r'\r\n\d+')
    
    # 공유 가능한 옵션 정의
    DEFAULT_PIPELINE_OPTIONS = PdfPipelineOptions(
        do_ocr=True,
        do_table_structure=True,
        ocr_options=EasyOcrOptions(lang=["en", "ko"])
        )
    
    def __init__(self, output_base_path: str = "../docs"):
        """
        Args:
            output_base_path: 파싱된 문서를 저장할 기본 경로
        """
        self.output_base_path = output_base_path
        self.embed_model=OllamaEmbeddings(base_url="http://localhost:11434", model="bge-m3:latest")
        self._ensure_output_directory()

    def _ensure_output_directory(self):
        """출력 디렉토리가 존재하는지 확인하고 없으면 생성"""
        Path(self.output_base_path).mkdir(parents=True, exist_ok=True)

    @staticmethod
    def normalize_newlines(text: str) -> str:
        """개행문자 정규화"""
        return DoclingParser.NEWLINE_PATTERN.sub('\n', text)

    def _setup_converter(self) -> DocumentConverter:
        """Docling 변환기 설정"""
        pipeline_options = self.DEFAULT_PIPELINE_OPTIONS
        
        return DocumentConverter(
            allowed_formats=[InputFormat.PDF],
            format_options={
                InputFormat.PDF: PdfFormatOption(
                    pipeline_options=pipeline_options,
                    backend=PyPdfiumDocumentBackend
                ),
            }
        )

    def _create_document_output_dir(self, lv1_cat: str, lv2_cat: str, lv3_cat: str, lv4_cat: str) -> str:
        """문서 출력 디렉토리 생성 및 경로 반환"""

        # 빈 값 제외
        enable_cats = [c for c in [lv1_cat, lv2_cat, lv3_cat, lv4_cat] if c]

        # Path.joinpath로 안전하게 경로 결합
        output_dir = Path(self.output_base_path)
        for cat in enable_cats:
            output_dir = output_dir / cat

        output_dir.mkdir(parents=True, exist_ok=True)
        return str(output_dir)
    
    
    def _get_md5_string(self, text:str):
        """문자열의 MD5 해시 반환"""
        return hashlib.md5(text.encode()).hexdigest()
    

    def _get_embedding(self, text:str):
        return self.embed_model.embed_query(text)

    def _process_single_page(self, loaded_docs, page_num: int, filename: str, filepath:str, lv1_cat: str, lv2_cat: str, lv3_cat: str, lv4_cat: str, first_sentence: str) -> Document:
        """단일 페이지 처리"""
        try:
            # Docling으로 마크다운 추출
            docling_text = loaded_docs.document.export_to_markdown(page_no=page_num + 1)
            
            # 텍스트 정제
            docling_text = docling_text.replace("<!-- image -->", "")
            docling_text = self.normalize_newlines(docling_text)
            docling_text = first_sentence + docling_text

            str_filepath = str(filepath).replace("\\", "/")
            hashed_filename = self._get_md5_string(filename)
            hashed_filepath = self._get_md5_string(str(filepath))
            hashed_page_content = self._get_md5_string(docling_text)
            embeddings = self._get_embedding(docling_text)

            # Document 객체 생성
            return Document(
                page_content=docling_text,
                metadata={
                    'id': str(uuid4()),
                    'filename': filename,
                    'filepath': str_filepath,
                    'hashed_filename': hashed_filename,
                    'hashed_filepath': hashed_filepath,
                    'hashed_page_content': hashed_page_content,
                    'lv1_cat': lv1_cat,
                    'lv2_cat': lv2_cat,
                    'lv3_cat': lv3_cat,
                    'lv4_cat': lv4_cat,
                    'embeddings': list(embeddings),
                    'page': str(page_num),
                    'status': 'success'
                    }
                )
        except Exception as e:
            print(f"페이지 {page_num} 처리 중 오류 발생: {e}")
            # 오류 발생 시 빈 문서 반환
            return Document(
                page_content=first_sentence + "\n[이 페이지를 처리하는 중 오류가 발생했습니다.]",
                metadata={
                    'id': str(uuid4()),
                    'filename': filename,
                    'filepath': str_filepath,
                    'hashed_filename': "",
                    'hashed_filepath': "",
                    'hashed_page_content': "",
                    'lv1_cat': lv1_cat,
                    'lv2_cat': lv2_cat,
                    'lv3_cat': lv3_cat,
                    'lv4_cat': lv4_cat,
                    'page': str(page_num),
                    'embeddings': [],
                    'error': str(e),
                    'status': "fail"
                    }
                )

    def _clear_folder(self, folder_path: str):
        """해당 폴더 안의 모든 파일과 하위 폴더를 삭제 (폴더는 유지)"""
        if not os.path.exists(folder_path):
            raise FileNotFoundError(f"폴더가 존재하지 않습니다: {folder_path}")

        for item in os.listdir(folder_path):
            item_path = os.path.join(folder_path, item)

            if os.path.isfile(item_path) or os.path.islink(item_path):
                os.unlink(item_path)  # 파일 또는 링크 삭제
            elif os.path.isdir(item_path):
                shutil.rmtree(item_path)  # 폴더 삭제
        print(f"폴더 내부가 모두 삭제되었습니다: {folder_path}")

    def parse_pdf_by_page(self, pdf_path: str, lv1_cat: str, lv2_cat: str, lv3_cat: str, lv4_cat: str) -> List[Document]:
        """
        PDF 문서를 페이지별로 파싱합니다.
        
        Args:
            pdf_path: PDF 파일 경로
            lv1_cat: 1차 카테고리
            lv2_cat: 2차 카테고리
            
        Returns:
            파싱된 Document 객체 리스트
        """
        # 경로 정규화
        pdf_path = Path(pdf_path).resolve()
        filename = pdf_path.name
        
        if not pdf_path.exists():
            raise FileNotFoundError(f"PDF 파일을 찾을 수 없습니다: {pdf_path}")

        # 초기 문장 생성
        enable_cats = [c for c in [lv1_cat, lv2_cat, lv3_cat, lv4_cat] if c] # 공백 Cat 제거
        first_sentence_cats = ",".join(enable_cats)
        first_sentence = f"This page explains {pdf_path.stem} that belongs to {first_sentence_cats} categories.\n"

        try:
            # 변환기 설정
            converter = self._setup_converter()
            loaded_docs = converter.convert(str(pdf_path))
            
            # PDF 페이지 수 확인
            with pdfplumber.open(pdf_path) as pdf:
                total_pages = len(pdf.pages)
                
            # 페이지별 처리
            docs = []
            for page_num in tqdm(range(total_pages), desc=f"파싱 중 - {filename}"):
                doc = self._process_single_page(loaded_docs, page_num, filename, pdf_path, lv1_cat, lv2_cat, lv3_cat, lv4_cat, first_sentence)
                docs.append(doc)
                time.sleep(0.1)  # 시스템 부하 방지

            # 결과 저장
            self._save_documents(docs, filename, lv1_cat, lv2_cat, lv3_cat, lv4_cat)
            
            return docs

        except Exception as e:
            print(f"PDF 파싱 중 오류 발생: {e}")
            raise

    def _save_documents(self, docs: List[Document], filename: str, 
                       lv1_cat: str, lv2_cat: str, lv3_cat: str, lv4_cat: str):
        """파싱된 문서를 파일로 저장"""
        output_dir = self._create_document_output_dir(lv1_cat, lv2_cat, lv3_cat, lv4_cat)
        parsed_filename = Path(filename).stem
        output_path = Path(output_dir) / f"{parsed_filename}.pkl"
        
        with open(output_path, 'wb') as file:  # 'wb' 모드로 변경
            pickle.dump(docs, file)
        
        print(f"문서 저장 완료: {output_path}")

    def _list_files_recursive(self, folder_path: str):
        """폴더 안의 파일을 재귀적으로 읽어서 제너레이터로 반환하는 함수"""
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                yield os.path.join(root, file)

    def _count_files(self, folder_path: str) -> int:
        """총 파일 개수 (메모리 부담 없음)"""
        count = 0
        for _, _, files in os.walk(folder_path):
            count += len(files)
        return count

    def batch_parse_pdfs(self, folder_path: str, remove_original: bool = False) -> List[List[Document]]:
        """
        폴더 내 모든 PDF 파일을 배치 처리합니다.
        
        Args:
            folder_path: PDF 파일들이 있는 폴더 경로
            remove_original: 파싱 후 원본 파일 삭제 여부
            
        Returns:
            각 PDF의 Document 객체 리스트를 포함하는 리스트
        """
        folder_path = Path(folder_path)
        pdf_files = self._list_files_recursive(folder_path=folder_path)
        
        if not pdf_files:
            print("처리할 PDF 파일이 없습니다.")
            return []

        all_docs = []
        for pdf_file in tqdm(pdf_files):

            file_path, file_name = os.path.split(pdf_file)
            target_file_path = file_path.split("uploaded")[1].replace("\\", "/")
            target_file_path = target_file_path.split("/")

            # 파일 폴더 경로에서 level cat 추출 (최대 4개까지)
            cats = defaultdict(str)
            for i in range(1,5,1):
                try: cats[f"lv{i}_cat"] = target_file_path[i]
                except: cats[f"lv{i}_cat"] = ""
            cats = dict(cats)
            lv1_cat, lv2_cat, lv3_cat, lv4_cat = cats["lv1_cat"], cats["lv2_cat"], cats["lv3_cat"], cats["lv4_cat"]

            try:
                docs = self.parse_pdf_by_page(str(pdf_file), lv1_cat, lv2_cat, lv3_cat, lv4_cat)
                all_docs.append(docs)
            except Exception as e:
                print(f"{pdf_file} 처리 중 오류: {e}")
                continue
        
        # 업로드 파일 삭제
        if remove_original in [True, "true", "True"] :
            self._clear_folder(folder_path=folder_path)

        return all_docs


# 사용 예시
if __name__ == "__main__":
    # 파서 인스턴스 생성
    parser = DoclingParser(output_base_path="../docs")
    
    # 배치 처리
    all_docs = parser.batch_parse_pdfs(
        folder_path="../docs/uploaded",
        remove_original=True
        )
    print(all_docs)