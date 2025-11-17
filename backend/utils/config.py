import os
from functools import lru_cache

class BaseConfig:
    """공통 설정"""
    ENV: str = os.getenv("ENV", "production")
    DEBUG: bool = os.getenv("DEBUG", "False").lower() == "true"
    REDIS_BROKER_URL: str = os.getenv("BROKER_URL", "redis://redis:6379/0")
    REDIS_BACKEND_URL: str = os.getenv("RESULT_BACKEND", "redis://redis:6379/1")
    REDIS_PUBSUB_URL: str = os.getenv("REDIS_PUBSUB_URL", "redis://redis:6379/2")
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "admin")
    POSTGRES_PW: str = os.getenv("POSTGRES_PW", "admin123")
    MARIA_USER: str = os.getenv("MARIA_USER", "admin")
    MARIA_PW: str = os.getenv("MARIA_PW", "admin123")
    OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

class DevConfig(BaseConfig):
    """개발 환경"""
    DEBUG: bool = True
    LOG_LEVEL: str = "DEBUG"

class ProdConfig(BaseConfig):
    """운영 환경"""
    DEBUG: bool = False
    LOG_LEVEL: str = "INFO"

@lru_cache
def get_config():
    """현재 ENV에 따라 Config 선택"""
    env = os.getenv("ENV", "production").lower()
    if env == "development":
        return DevConfig()
    return ProdConfig()
