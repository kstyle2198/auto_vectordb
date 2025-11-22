import json
import pickle
import psycopg2
from psycopg2.extras import execute_batch
import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime
from tqdm import tqdm
import sys
from pathlib import Path
utils_path = Path(__file__).parent.parent
sys.path.append(str(utils_path))
# print(utils_path)

from utils.config import get_config
from utils.setlogger import setup_logger
config = get_config()
logger = setup_logger(f"{__name__}", level=config.LOG_LEVEL)

class PostgresPipeline:
    def __init__(self, host="localhost", database="mydb", user=config.POSTGRES_USER, password=config.POSTGRES_PW):
        """데이터베이스 연결 정보를 초기화합니다."""
        self.db_config = {
            "host": host,
            "database": database,
            "user": user,
            "password": password
        }

    def _get_db_connection(self):
        """데이터베이스 연결을 반환합니다."""
        try:
            logger.info("START - POSTGRESS DB CONNECTION")
            conn = psycopg2.connect(**self.db_config)
            return conn
        except psycopg2.Error as e:
            logger.error(f"CONNECTION ERROR: {e}")
            raise

    def get_all_tables(self):
        """데이터베이스의 모든 테이블 이름을 조회합니다."""
        conn = None
        try:
            logger.info("START - GET TABLE NAMES")
            # DB 연결
            conn = self._get_db_connection()
            cur = conn.cursor()

            # 모든 테이블 이름 조회 (시스템 테이블 제외)
            query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            ORDER BY table_name;
            """
            
            cur.execute(query)
            tables = cur.fetchall()

            return [table[0] for table in tables]

        except (Exception, psycopg2.DatabaseError) as error:
            logger.error("오류 발생:", error)
            return []
        finally:
            if conn is not None:
                cur.close()
                conn.close()

    def drop_table(self, table_name: str):
        """지정된 테이블을 삭제합니다."""
        conn = None
        try:
            logger.info("DROP TABLE")
            conn = self._get_db_connection()
            cursor = conn.cursor()
            
            # 테이블 삭제 SQL 실행
            drop_query = f"DROP TABLE IF EXISTS {table_name} CASCADE;"
            cursor.execute(drop_query)
            conn.commit()
            
            logger.info(f"테이블 '{table_name}'이(가) 성공적으로 삭제되었습니다.")
            
        except psycopg2.Error as e:
            if conn:
                conn.rollback()
            logger.error(f"테이블 삭제 오류: {e}")
            raise
        finally:
            if conn:
                cursor.close()
                conn.close()

    def create_table(self, table_name: str, columns_config: List[Dict[str, str]]):
        """
        PostgreSQL 데이터베이스에 새로운 테이블을 생성합니다.
        
        Args:
            table_name (str): 생성할 테이블 이름
            columns_config (List[Dict]): 컬럼 구성 정보
                예: [
                    {"name": "id", "type": "SERIAL PRIMARY KEY"},
                    {"name": "name", "type": "VARCHAR(100) NOT NULL"},
                    {"name": "created_at", "type": "TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP"}
                ]
        """
        conn = None
        try:
            logger.info("CREATE TABLE")
            # 데이터베이스 연결
            conn = self._get_db_connection()
            cur = conn.cursor()

            # 컬럼 정의 생성
            column_definitions = []
            for column in columns_config:
                column_definitions.append(f"{column['name']} {column['type']}")
            
            columns_sql = ",\n                ".join(column_definitions)

            # 실행할 SQL 쿼리 (테이블 생성)
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {columns_sql}
            );
            """

            # SQL 쿼리 실행
            cur.execute(create_table_query)

            # 변경사항을 데이터베이스에 커밋(commit)
            conn.commit()

            logger.info(f"'{table_name}' 테이블이 성공적으로 생성되었습니다.")

        except (Exception, psycopg2.DatabaseError) as error:
            logger.error("Error while creating PostgreSQL table", error)
            if conn:
                conn.rollback()
        finally:
            # 연결 종료
            if conn is not None:
                cur.close()
                conn.close()
                logger.info("PostgreSQL connection is closed.")

    def _reform_csv_data(self, csv_path: str):
        """엑셀 데이터를 재구성합니다."""
        logger.info("START - Reform CSV DATA")
        df = pd.read_csv(csv_path, encoding="utf-8-sig")
        df.fillna('', inplace=True)
        # NaN 값을 명시적으로 빈 문자열로 변환
        df = df.where(pd.notnull(df), '')
        reformed_result = [tuple(row) for row in df.values]
        return reformed_result

    def _chunked_data(self, data: list, chunk_size: int):
        """데이터를 청크 단위로 나눕니다."""
        for i in range(0, len(data), chunk_size):
            yield data[i:i + chunk_size]

    def insert_data_from_csv(self, table_name: str, csv_path: str, columns: list, chunk_size: int = 100):
        """
        지정된 테이블에 데이터를 삽입합니다.
        
        Args:
            table_name (str): 데이터를 삽입할 테이블 이름
            columns (List[str]): 컬럼 이름 목록
            data (List[tuple]): 삽입할 데이터 (튜플 리스트)
            chunk_size (int): 한 번에 삽입할 청크 크기
        """
        conn = None
        cur = None
        
        try:
            logger.info("START - INSERT DATA")
            # 데이터베이스 연결
            conn = self._get_db_connection()
            cur = conn.cursor()

            # 컬럼 이름을 SQL 쿼리 형식으로 변환
            columns_sql = ", ".join(columns)
            placeholders = ", ".join(["%s"] * len(columns))
            
            # SQL 쿼리 생성
            sql = f"INSERT INTO {table_name} ({columns_sql}) VALUES ({placeholders})"

            # Data Reform
            data = self._reform_csv_data(csv_path=csv_path)

            total_inserted = 0
            for i, chunk in enumerate(self._chunked_data(data, chunk_size)):
                logger.info(f">>> 청크 데이터 개수: {len(chunk)}")
                try:
                    # execute_batch를 사용하여 배치 삽입 (성능 향상)
                    execute_batch(cur, sql, chunk)
                    conn.commit()
                    
                    total_inserted += len(chunk)
                    logger.info(f"청크 {i+1} - {len(chunk)}개의 데이터 삽입 완료 (총 {total_inserted}개)")
                    
                except Exception as chunk_error:
                    logger.error(f"청크 {i+1} 삽입 중 오류: {chunk_error}")
                    conn.rollback()
                    # 실패한 청크의 데이터 로깅 (디버깅용)
                    logger.error(f"실패한 청크 데이터: {len(chunk)} 개")  # 실패한 청크 개수
                    logger.error(f"실패한 청크 데이터: {chunk[:1]}")  # 처음 1개 행만 로깅
                    
                    continue  # 다음 청크로 계속 진행

            logger.info(f"모든 데이터 삽입 완료: 총 {total_inserted}개의 행")

        except Exception as error:
            logger.error(f"전체 처리 중 오류 발생: {error}")
            if conn:
                conn.rollback()
        finally:
            # 리소스 정리
            if cur:
                cur.close()
            if conn:
                conn.close()
                logger.info("데이터베이스 연결 종료")

    def insert_data_from_pickle(self, table_name: str, pickle_path: str):
        """
        """
        conn = None
        cur = None
        pickle_path = pickle_path.replace("\\", "/")

        with open(pickle_path, 'rb') as f:
            docs = pickle.load(f)

        # 컬럼 이름을 SQL 쿼리 형식으로 변환
        columns = ['id', 'page_content', 'filename', 'filepath','hashed_filename', 'hashed_filepath', 'hashed_page_content',
                    'page', 'lv1_cat', 'lv2_cat', 'lv3_cat', 'lv4_cat', 'embeddings', 'created_at', 'updated_at']
        columns_sql = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        
        # SQL 쿼리 생성
        sql = f"INSERT INTO {table_name} ({columns_sql}) VALUES ({placeholders})"
        try:
            logger.info("START - INSERT DATA")
            # 데이터베이스 연결
            conn = self._get_db_connection()
            cur = conn.cursor()

            # Convert to dictionary
            data = [{"page_content": doc.page_content,"metadata": doc.metadata} for doc in docs]
            # print(len(data))

            # 데이터 정규화
            normalized = []
            for row in tqdm(data):
                new_row = [
                    row["metadata"].get("id"),
                    row.get("page_content"),
                    row["metadata"].get("filename"),
                    row["metadata"].get("filepath"),
                    row["metadata"].get("hashed_filename"),
                    row["metadata"].get("hashed_filepath"),
                    row["metadata"].get("hashed_page_content"),
                    row["metadata"].get("page"),
                    row["metadata"].get("lv1_cat"),
                    row["metadata"].get("lv2_cat"),
                    row["metadata"].get("lv3_cat"),
                    row["metadata"].get("lv4_cat"),
                    row["metadata"].get("embeddings"),
                    row.get("created_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")),
                    row.get("updated_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
                    ]

                normalized.append(new_row)

            execute_batch(cur, sql, normalized)
            conn.commit()
                    
        except Exception as error:
            logger.error(f"전체 처리 중 오류 발생: {error}")
            if conn:
                conn.rollback()
        finally:
            # 리소스 정리
            if cur:
                cur.close()
            if conn:
                conn.close()
                logger.info("데이터베이스 연결 종료")

    def select_all_data(self, table_name: str, limit: Optional[int] = 10, order_by: str = "id"):
        """지정된 테이블의 10게 데이터를 조회합니다."""
        conn = None
        try:
            logger.info("START - SELECT DATA")
            # DB 연결
            conn = self._get_db_connection()
            cur = conn.cursor()

            # SELECT 쿼리 생성
            if limit:
                query = f"SELECT * FROM {table_name} ORDER BY {order_by} LIMIT %s;"
                cur.execute(query, (limit,))
            else:
                query = f"SELECT * FROM {table_name} ORDER BY {order_by};"
                cur.execute(query)

            # 조회된 모든 데이터를 한 번에 가져오기
            results = cur.fetchall()
            return results

        except (Exception, psycopg2.DatabaseError) as error:
            logger.error("오류 발생:", error)
            return []
        finally:
            if conn is not None:
                cur.close()
                conn.close()

    def get_row_by_hashed_filepath(self, table_name, hashed_filepath):
        """
        PostgreSQL에서 특정 hashed_filepath의 데이터를 조회하는 함수
        """
        conn = None
        try:
            conn = self._get_db_connection()
            cur = conn.cursor()

            query = f"SELECT * FROM {table_name} WHERE hashed_filepath = %s"
            cur.execute(query, (hashed_filepath,))
            
            result = cur.fetchall()
            return result

        except Exception as e:
            print("Error:", e)
            return None
        finally:
            if conn:
                conn.close()
    
    def get_unique_hashed_filepath(self, table_name):
        """
        PostgreSQL에서 특정 hashed_filepath 데이터만 조회하는 함수 (중복 제거)
        """
        conn = None
        try:
            conn = self._get_db_connection()
            cur = conn.cursor()

            query = f"SELECT hashed_filepath FROM {table_name}"
            cur.execute(query)
            
            rows = cur.fetchall()
            return list({row[0] for row in rows})

        except Exception as e:
            print("Error:", e)
            return None
        finally:
            if conn:
                conn.close()

    def delete_data_by_id(self, table_name: str, id_column: str, record_id: int):
        """특정 ID를 가진 레코드를 테이블에서 삭제합니다."""
        conn = None
        deleted_rows = 0
        try:
            logger.info("START - DELETE DATA")
            # DB 연결
            conn = self._get_db_connection()
            cur = conn.cursor()

            # DELETE 쿼리 실행
            delete_query = f"DELETE FROM {table_name} WHERE {id_column} = %s;"
            cur.execute(delete_query, (record_id,))

            # 삭제된 행 수 확인
            deleted_rows = cur.rowcount

            # 변경사항 커밋
            conn.commit()

            if deleted_rows > 0:
                logger.info(f"{table_name} 테이블에서 {id_column} {record_id}인 레코드가 성공적으로 삭제되었습니다.")
            else:
                logger.warning(f"{table_name} 테이블에서 {id_column} {record_id}인 레코드를 찾을 수 없습니다.")

        except (Exception, psycopg2.DatabaseError) as error:
            logger.error("오류 발생:", error)
            # 오류 발생 시 롤백
            if conn is not None:
                conn.rollback()
        finally:
            if conn is not None:
                cur.close()
                conn.close()
        
        return deleted_rows
    

if __name__ == "__main__":

    pg = PostgresPipeline()
    pickle_path = "D:/auto_vectordb/backend/docs/project01/cat_01/cat_01_01/FWG.pkl"
    pg.insert_data_from_pickle(table_name="pjt_001", pickle_path=pickle_path)
    pass

