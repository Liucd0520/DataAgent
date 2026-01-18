
import sys
from pathlib import Path
# 将项目根目录添加到 Python 路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))



import re
from typing import List, Dict
from langchain_community.utilities import SQLDatabase
from DataAgent.datasource.schema_parse import parse_multiple_tables_schemas




def extract_table_schemas(table_schemas: str) -> Dict[str, str]:
    """
    从 get_table_info() 返回的字符串中提取每个表的 schema

    Args:
        table_schemas: get_table_info() 返回的字符串，由 '\n\n'.join() 得到

    Returns:
        字典，key 是表名，value 是该表的 CREATE TABLE 语句

    原理:
        1. 每个表的 schema 以 CREATE TABLE 开头
        2. 表与表之间用 \n\n 分隔
        3. 联合这两个规则可以准确定位每个表的边界
    """
    # 正则表达式匹配 CREATE TABLE 语句，提取表名
    # 支持 MySQL 的反引号、PostgreSQL/DM 的双引号、SQL Server 的方括号
    pattern = r'CREATE\s+TABLE\s+(?:[\w"]+\.)?[`\["]?([\w"]+)[`\")]?\s*\('

    table_dict = {}
    # 找到所有 CREATE TABLE 的位置和表名
    create_table_positions = []
    for match in re.finditer(pattern, table_schemas, re.IGNORECASE):
        table_name = match.group(1).strip('"')
        create_table_positions.append((match.start(), table_name))

    # 基于位置提取每个表的完整 schema
    for i, (start_pos, table_name) in enumerate(create_table_positions):
        # 如果不是最后一个表，找到下一个 CREATE TABLE 前的 \n\n
        if i < len(create_table_positions) - 1:
            next_start = create_table_positions[i + 1][0]
            # 从下一个 CREATE TABLE 向前查找 \n\n
            # 回溯查找 \n\n 分隔符
            end_pos = next_start
            while end_pos > start_pos and end_pos - 2 >= start_pos:
                if table_schemas[end_pos - 2:end_pos] == '\n\n':
                    break  # 找到分隔符，当前表到此为止
                end_pos -= 1
        else:
            # 最后一个表，直接到末尾
            end_pos = len(table_schemas)

        # 提取并清理首尾空白
        full_statement = table_schemas[start_pos:end_pos].strip()
        table_dict[table_name] = full_statement

    return table_dict


def schema_obtain(business_db: SQLDatabase, table_names: List[str]) -> Dict[str, Dict]:
    """
    获取指定表的 schema 信息并解析

    Args:
        business_db: SQLDatabase 实例
        table_names: 表名列表

    Returns:
        字典，key 是表名，value 是解析后的表信息字典，包含:
        - table_name: 表名
        - table_comment: 表级别的注释
        - columns: 完整字段信息列表
        - column_names: 字段名列表
        - column_types: 字段类型字典
        - sample_data_raw: 示例数据原始字符串
    """
    # 1. 获取原始 schema 字符串（多个表合并的字符串）
    raw_schema_str = business_db.get_table_info(table_names=table_names)
    # print(raw_schema_str)
    
    # 2. 提取每个表的 CREATE TABLE 语句（按表分割）
    raw_schemas_by_table = extract_table_schemas(raw_schema_str)

    # 3. 解析每个表的 schema（结构化数据）
    parsed_schemas_by_table = parse_multiple_tables_schemas(raw_schemas_by_table)

    return raw_schemas_by_table, parsed_schemas_by_table
    

# 测试代码
if __name__ == '__main__':
    # uri =  f'postgresql+pg8000://postgres:liucd123@127.0.0.1:5432/postgres'
    # business_db = SQLDatabase.from_uri(uri)
    # table_names = ['shanghai']
    # res = schema_obtain(business_db, table_names)
    # print(res)

 
    uri = f'mysql+mysqlconnector://root:liucd123@127.0.0.1:3306/12345'
    business_db = SQLDatabase.from_uri(uri)
    table_names = ['pudong', 'shanghai', 'hongkou']

    raw_schemas_by_table, parsed_schemas_by_table = schema_obtain(business_db, table_names)
    print(business_db.dialect)
    # print('#' * 100)
    # print(parsed_schemas_by_table)
