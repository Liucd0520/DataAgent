
import sys
from pathlib import Path
# 将项目根目录添加到 Python 路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


from typing import Literal
from langchain_community.utilities import SQLDatabase
from DataAgent.datasource.util import decrypt 
from typing import Optional, Dict, Any
import ast 
import datetime 
from decimal import Decimal
import decimal


def get_database_uri(db_type, host, port, username, password, database):
    """根据数据库类型生成连接 URI"""
    
    db_configs = {
        'mysql': f'mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database}',
        'postgresql': f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}',
        # pg8000 对中文支持更好，可以解决编码问题
        'dm': f'dm+dmPython://{username}:{password}@{host}:{port}/{database}',
    }
    
    db_key = db_type.lower()
    if db_key not in db_configs:
        raise ValueError(
            f"不支持的数据库类型: '{db_type}'。"
            f"当前仅支持: {', '.join(db_configs.keys())}"
        )
    
    return db_configs[db_key]


def obtain_database_connect_config(param_uri: str, db_id: int, )-> Optional[SQLDatabase]:
    """
    从配置库读取业务数据库连接配置并建立连接
    Args:
        param_uri: 配置库的连接 URI
        db_id: 业务库配置 ID
        
    Returns:
        SQLDatabase 对象，如果配置不存在或出错返回 None
    Raises:
        ValueError: 配置无效时抛出
    """
     
    # 读取配置表 database_info
    param_db = SQLDatabase.from_uri(param_uri)

    sql_command = f'SELECT * FROM database_info where id = :db_id;'
    conf_fields_str = param_db.run(sql_command, include_columns=True, parameters={'db_id': db_id})
    try:
        conf_fields = eval(conf_fields_str)
    except Exception as e:
            raise ValueError(f"配置数据格式错误: {e}")
    
    # 验证配置存在且唯一
    if not conf_fields:
        raise ValueError(f"未找到 ID 为 {db_id} 的数据库配置")
    
    # 判断业务库的配置信息只会有1个
    if len(conf_fields) != 1:
        raise ValueError(f"找到 {len(conf_fields)} 条配置，期望仅 1 条")
    
    conf_field = conf_fields[0]
    
    # 验证必需字段
    required_fields = ['host', 'port', 'user_name', 'database_type', 
                        'password', 'database_name']
    missing_fields = [f for f in required_fields if f not in conf_field]
    if missing_fields:
        raise ValueError(f"配置缺少必需字段: {', '.join(missing_fields)}")
        

    # 业务库的连接
    host = conf_field['host']
    port = conf_field['port']
    user_name = conf_field['user_name']
    db_type = conf_field['database_type']

    try:
        password = decrypt(conf_field['password']).replace('@', '%40')
    except Exception as e:
        raise ValueError(f"密码解密失败: {e}")
    
    database_name = conf_field['database_name']
    
    # 获取数据库引擎
    business_db_uri = get_database_uri(db_type, host, port, user_name, password, database_name)
    business_db = SQLDatabase.from_uri(business_db_uri)

    return business_db


if __name__ == '__main__':
    param_uri="mysql+mysqlconnector://root:liucd123@172.31.24.111:3307/xingchentwo"
    db_id=4
    obtain_database_connect_config(param_uri=param_uri, db_id=db_id)