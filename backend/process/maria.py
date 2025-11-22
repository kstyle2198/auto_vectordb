import json
import pickle
import pymysql
import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime
from tqdm import tqdm
import sys
from pathlib import Path

utils_path = Path(__file__).parent.parent
sys.path.append(str(utils_path))

from utils.config import get_config
from utils.setlogger import setup_logger
config = get_config()
logger = setup_logger(f"{__name__}", level=config.LOG_LEVEL)


class MariaPipeline:
    def __init__(self, host="localhost", database="maria_db", user=config.MARIA_USER, password=config.MARIA_PW):
        """데이터베이스 연결 정보를 초기화합니다."""
        self.db_config = {
            "host": host,
            "db": database,
            "user": user,
            "password": password,
            "charset": "utf8mb4",
            "autocommit": False
        }

    def _get_db_connection(self):
        """MariaDB 데이터베이스 연결을 반환합니다."""
        try:
            logger.info("START - MARIADB CONNECTION")
            conn = pymysql.connect(**self.db_config)
            return conn
        except pymysql.MySQLError as e:
            logger.error(f"CONNECTION ERROR: {e}")
            raise

    def get_all_tables(self):
        """데이터베이스의 모든 테이블 이름을 조회합니다."""
        conn = None
        try:
            logger.info("START - GET TABLE NAMES")
            conn = self._get_db_connection()
            cur = conn.cursor()

            query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s 
            ORDER BY table_name;
            """
            
            cur.execute(query, (self.db_config["db"],))
            tables = cur.fetchall()

            return [table[0] for table in tables]

        except pymysql.MySQLError as error:
            logger.error(f"오류 발생: {error}")
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
            
            drop_query = f"DROP TABLE IF EXISTS `{table_name}`;"
            cursor.execute(drop_query)
            conn.commit()
            
            logger.info(f"테이블 '{table_name}' 삭제 완료")
            
        except pymysql.MySQLError as e:
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
        MariaDB 테이블 생성
        SERIAL → AUTO_INCREMENT PRIMARY KEY 로 변경됨
        """
        conn = None
        try:
            logger.info("CREATE TABLE")
            conn = self._get_db_connection()
            cur = conn.cursor()

            column_definitions = []
            for column in columns_config:
                col_type = column['type']
                # PostgreSQL의 SERIAL 을 MariaDB에 맞게 변경
                col_type = col_type.replace("SERIAL", "INT AUTO_INCREMENT")
                col_type = col_type.replace("TIMESTAMP WITH TIME ZONE", "TIMESTAMP")
                column_definitions.append(f"`{column['name']}` {col_type}")
            
            columns_sql = ",\n                ".join(column_definitions)

            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS `{table_name}` (
                {columns_sql}
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """

            cur.execute(create_table_query)
            conn.commit()

            logger.info(f"{table_name} 테이블 생성 완료")

        except pymysql.MySQLError as error:
            logger.error(f"Error while creating table: {error}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                cur.close()
                conn.close()

    def _reform_csv_data(self, csv_path: str):
        """CSV 데이터를 재구성합니다."""
        logger.info("START - Reform CSV DATA")
        df = pd.read_csv(csv_path, encoding="utf8")
        df.fillna('', inplace=True)
        return [tuple(row) for row in df.values]

    def _chunked_data(self, data: list, chunk_size: int):
        for i in range(0, len(data), chunk_size):
            yield data[i:i + chunk_size]

    def insert_data_from_csv(self, table_name: str, csv_path: str, columns: list, chunk_size: int = 100):
        """CSV 데이터를 MariaDB에 삽입"""
        conn = None
        cur = None
        
        try:
            logger.info("START - INSERT DATA FROM CSV")
            conn = self._get_db_connection()
            cur = conn.cursor()

            columns_sql = ", ".join([f"`{c}`" for c in columns])
            placeholders = ", ".join(["%s"] * len(columns))
            
            sql = f"INSERT INTO `{table_name}` ({columns_sql}) VALUES ({placeholders})"

            data = self._reform_csv_data(csv_path)

            total_inserted = 0
            for i, chunk in enumerate(self._chunked_data(data, chunk_size)):
                try:
                    cur.executemany(sql, chunk)
                    conn.commit()
                    total_inserted += len(chunk)
                except Exception as e:
                    logger.error(f"Chunk error: {e}")
                    conn.rollback()
                    continue

            logger.info(f"총 {total_inserted}개 데이터 삽입 완료")

        except Exception as error:
            logger.error(f"전체 처리 중 오류 발생: {error}")
            if conn:
                conn.rollback()
        finally:
            if cur: cur.close()
            if conn:
                conn.close()

    def insert_data_from_pickle(self, table_name: str, pickle_path: str):
        """pickle 기반 insert"""
        conn = None
        cur = None
        pickle_path = pickle_path.replace("\\", "/")
        with open(pickle_path, 'rb') as f:
            docs = pickle.load(f)
        
        columns = ['id', 'page_content', 'filename', 'filepath','hashed_filename', 'hashed_filepath',
                    'hashed_page_content', 'page', 'lv1_cat', 'lv2_cat', 'lv3_cat', 'lv4_cat',
                    'embeddings', 'created_at', 'updated_at']

        columns_sql = ", ".join([f"`{c}`" for c in columns])
        placeholders = ", ".join(["%s"] * len(columns))

        sql = f"INSERT INTO `{table_name}` ({columns_sql}) VALUES ({placeholders})"

        try:
            logger.info("START - INSERT FROM PICKLE")
            conn = self._get_db_connection()
            cur = conn.cursor()

            data = [{"page_content": doc.page_content, "metadata": doc.metadata} for doc in docs]
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
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ]
                normalized.append(new_row)

            cur.executemany(sql, normalized)
            conn.commit()

        except Exception as error:
            logger.error(f"INSERT ERROR: {error}")
            if conn:
                conn.rollback()
        finally:
            if cur: cur.close()
            if conn:
                conn.close()

    def select_all_data(self, table_name: str, limit: Optional[int] = 10, order_by: str = "id"):
        """전체 조회"""
        conn = None
        try:
            conn = self._get_db_connection()
            cur = conn.cursor()

            if limit:
                query = f"SELECT * FROM `{table_name}` ORDER BY `{order_by}` LIMIT %s"
                cur.execute(query, (limit,))
            else:
                query = f"SELECT * FROM `{table_name}` ORDER BY `{order_by}`"
                cur.execute(query)

            return cur.fetchall()

        except pymysql.MySQLError as error:
            logger.error(f"오류 발생: {error}")
            return []
        finally:
            if conn:
                cur.close()
                conn.close()

    def delete_data_by_id(self, table_name: str, id_column: str, record_id: int):
        """ID 기준 삭제"""
        conn = None
        try:
            conn = self._get_db_connection()
            cur = conn.cursor()

            delete_query = f"DELETE FROM `{table_name}` WHERE `{id_column}` = %s"
            cur.execute(delete_query, (record_id,))
            conn.commit()

            return cur.rowcount

        except pymysql.MySQLError as error:
            logger.error(f"삭제 오류: {error}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                cur.close()
                conn.close()
        return 0


if __name__ == "__main__":
    db = MariaPipeline()
    pickle_path = "D:/auto_vectordb/backend/docs/parsed/project01/cat_01/cat_01_01/FWG.pkl"
    db.insert_data_from_pickle(table_name="test_001", pickle_path=pickle_path)
