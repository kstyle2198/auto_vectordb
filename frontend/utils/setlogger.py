"""
컬러 로그 시스템 모듈

이 모듈은 Python의 logging 모듈을 확장하여 로그를
- 파일에는 일반 텍스트 형태로 저장
- 터미널에는 ANSI 컬러 코드가 적용된 메시지로 출력
할 수 있도록 지원합니다.
"""

import os
import logging
import datetime as dt


# 색상 코드 정의 (ANSI Escape Code)
LOG_COLORS = {
    "DEBUG": "\033[37m",    # 회색
    "INFO": "\033[36m",     # 청록색
    "WARNING": "\033[33m",  # 노란색
    "ERROR": "\033[31m",    # 빨간색
    "CRITICAL": "\033[41m", # 빨간 배경
}
RESET_COLOR = "\033[0m"


class ColorFormatter(logging.Formatter):
    """
    로그 메시지에 ANSI 색상 코드를 적용하는 Formatter 클래스.

    Methods
    -------
    format(record: logging.LogRecord) -> str
        주어진 로그 레코드를 포맷팅하고,
        로그 레벨에 따라 색상을 입혀 반환합니다.
    """

    def format(self, record):
        log_color = LOG_COLORS.get(record.levelname, RESET_COLOR)
        message = super().format(record)
        return f"{log_color}{message}{RESET_COLOR}"


def setup_logger(log_name: str, level=logging.DEBUG) -> logging.Logger:
    """
    지정한 이름과 레벨로 로거(Logger)를 생성하고 반환합니다.

    - 로그는 `./logging/` 디렉터리에 일자별 파일로 저장됩니다.
    - 터미널 출력은 ANSI 색상이 적용됩니다.

    Parameters
    ----------
    log_name : str
        로거의 이름 (ex: "my_app").
    level : int, optional
        로깅 레벨 (기본값: logging.DEBUG).

    Returns
    -------
    logging.Logger
        파일 및 컬러 콘솔 출력이 동시에 적용된 로거 객체.
    """
    # create logger file folder 
    make_folder = "./logs/"
    os.makedirs(make_folder, exist_ok=True)
    today = dt.date.today()
    filename = f"{make_folder}{today.month:02d}-{today.day:02d}-{today.year}.log"

    # create logger
    logger = logging.getLogger(log_name)
    logger.setLevel(logging.DEBUG)

    # formatter (파일에는 색상 없는 기본 formatter)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    color_formatter = ColorFormatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # file handler
    file_handler = logging.FileHandler(filename, encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(file_formatter)

    # stream handler (console with color)
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(level)
    stream_handler.setFormatter(color_formatter)

    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

    return logger


# 예시 실행
if __name__ == "__main__":
    log = setup_logger("my_app")
    log.debug("이건 DEBUG 메시지 (회색)")
    log.info("이건 INFO 메시지 (청록색)")
    log.warning("이건 WARNING 메시지 (노란색)")
    log.error("이건 ERROR 메시지 (빨간색)")
    log.critical("이건 CRITICAL 메시지 (빨간 배경)")
