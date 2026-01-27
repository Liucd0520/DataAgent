
from langchain_community.utilities import SQLDatabase



def get_database_uri(db_type, host, port, username, password, database):
    """根据数据库类型生成连接 URI"""
    
    db_configs = {
        'mysql': f'mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database}',
        'postgresql': f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}',
        # pg8000 对中文支持更好，可以解决编码问题
        'dm': f'dm+dmPython://{username}:{password}@{host}:{port}/{database}',
        'teledb': f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}',       
    }
    
    db_key = db_type.lower()
    if db_key not in db_configs:
        raise ValueError(
            f"不支持的数据库类型: '{db_type}'。"
            f"当前仅支持: {', '.join(db_configs.keys())}"
        )
    
    return db_configs[db_key]


business_db_uri = get_database_uri(db_type='teledb', host='172.31.24.112', port=5452, username='pguser', password='my-secret-pw', database='test')
business_db = SQLDatabase.from_uri(business_db_uri)

res = business_db.run('select * from "shanghai" limit 10')
print(res)
