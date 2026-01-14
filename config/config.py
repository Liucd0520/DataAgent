
"""
配置模块
"""
import os
from pathlib import Path

# 项目根目录
BASE_DIR = Path(__file__).resolve().parent.parent

# 数据库配置
DATABASE = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', ''),
    'database': os.getenv('DB_NAME', 'dataagent'),
    'charset': 'utf8mb4'
}

# API 配置
API_CONFIG = {
    'host': os.getenv('API_HOST', '0.0.0.0'),
    'port': int(os.getenv('API_PORT', 8000)),
    'debug': os.getenv('API_DEBUG', 'False').lower() == 'true'
}

# 日志配置
LOG_DIR = BASE_DIR / 'logs'
LOG_DIR.mkdir(exist_ok=True)

LOG_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        },
    },
    'handlers': {
        'file': {
            'class': 'logging.FileHandler',
            'filename': str(LOG_DIR / 'app.log'),
            'formatter': 'standard',
        },
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
        },
    },
    'root': {
        'handlers': ['console', 'file'],
        'level': 'INFO',
    },
}

# 数据源配置
DATASOURCE_CONFIG = {
    'timeout': 30,
    'pool_size': 5,
    'max_overflow': 10
}

# 知识库配置
KNOWLEDGE_CONFIG = {
    'embedding_model': 'text-embedding-ada-002',
    'chunk_size': 1000,
    'chunk_overlap': 200
}

# 查询配置
QUERY_CONFIG = {
    'max_results': 100,
    'timeout': 60
}

# 报告配置
REPORT_CONFIG = {
    'template_dir': BASE_DIR / 'templates',
    'output_dir': BASE_DIR / 'outputs'
}

# 工作流配置
WORKFLOW_CONFIG = {
    'max_concurrent_tasks': 5,
    'task_timeout': 300
}

# 语义字段配置
SEMANTIC_FIELDS = ["内容描述"]

# Schema 文件路径
SCHEMA_FILE = BASE_DIR / 'schema.txt'

# 兼容旧变量名
sementic_field = SEMANTIC_FIELDS

def get_schema():
    """读取 schema 配置"""
    if SCHEMA_FILE.exists():
        with open(SCHEMA_FILE, 'r', encoding='utf-8') as f:
            return f.read()
    return ""

schema = get_schema()