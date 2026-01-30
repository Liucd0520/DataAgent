
import sys
from pathlib import Path
# 将项目根目录添加到 Python 路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import re
from typing import Dict, List, Tuple, Optional
from DataAgent.datasource.chain import translate_chain


def _contains_chinese(text: str) -> bool:
    """
    检测文本是否包含中文字符

    Args:
        text: 待检测的文本

    Returns:
        bool: True 表示包含中文，False 表示不包含中文
    """
    return bool(re.search(r'[\u4e00-\u9fff]', text))


def _translate_field_name(field_name: str) -> str:
    """
    翻译字段名为英文

    Args:
        field_name: 字段名（可能是中文）

    Returns:
        str: 翻译后的英文字段名，如果字段名本身不是中文则返回原值
    """
    # 如果不包含中文，直接返回原值
    if not _contains_chinese(field_name):
        return field_name

    try:
        # 调用翻译链
        result = translate_chain.invoke({"field_name": field_name})
        translated = result.content.strip()

        # 如果翻译结果为空，返回原字段名
        if not translated:
            print(f"翻译字段名 '{field_name}' 时出错: 输出为空！")
            return field_name

        return translated
    except Exception as e:
        print(f"翻译字段名 '{field_name}' 时出错: {str(e)}")
        return field_name


def parse_table_schema(schema: str) -> Dict[str, any]:
    """
    解析表 schema，提取字段信息和示例数据

    Args:
        schema: CREATE TABLE 语句字符串（包含示例数据）

    Returns:
        字典，包含:
        - table_name: 表名
        - table_comment: 表级别的注释
        - columns: 完整字段信息列表，每个字段包含 name, type, comment, english_name, full_definition
        - column_names: 仅字段名列表 (List[str])
        - column_types: {字段名: 类型} 的字典 (Dict[str, str])
        - sample_data_raw: 示例数据的原始字符串（未解析）

    注意:
        - english_name: 字段名的英文翻译，如果字段名本身不是中文则返回原字段名
    """
    result = {
        'table_name': '',
        'table_comment': '',     # 表级别的注释
        'columns': [],           # 完整字段信息列表
        'column_names': [],      # 仅字段名列表
        'column_types': {},      # {字段名: 类型} 的字典
        'sample_data_raw': ''    # 示例数据的原始字符串
    }
    # print(schema)

    # 1. 提取表名和表级别的 COMMENT
    table_name_match = re.search(r'CREATE\s+TABLE\s+(?:[\w"]+\.)?[`\["]?([\w"]+)[`\")]?\s*\(', schema, re.IGNORECASE)
    if table_name_match:
        result['table_name'] = table_name_match.group(1).strip('"')

    # 提取表级别的 COMMENT（在 CREATE TABLE 结束后的 COMMENT='...'）
    # 格式：)DEFAULT CHARSET=utf8mb4 COMMENT='这是浦东数据' ENGINE=InnoDB
    table_comment_match = re.search(r'\)\s*DEFAULT\s+CHARSET[^\n]*?COMMENT=[\'"]([^\'\"]+)[\'\"]', schema, re.IGNORECASE)
    if table_comment_match:
        result['table_comment'] = table_comment_match.group(1).strip()

    # 2. 分离 CREATE TABLE 部分和示例数据部分

    parts = schema.split('\n\n/*')
    schema_body = parts[0].strip()


    # 3. 提取括号内的字段定义部分
    # 匹配到最后一个右括号（可能是 )COLLATE... 或 ）
    columns_match = re.search(r'\(([\s\S]*?)\)\s*(?:COLLATE|ENGINE|/\*)', schema_body)
    if not columns_match:
        # 如果没有 COLLATE/ENGINE，尝试匹配到最后一个右括号
        columns_match = re.search(r'\(([\s\S]*?)\)\s*$', schema_body)

    if not columns_match:
        return result

    columns_text = columns_match.group(1)

    # 4. 解析每个字段
    # 策略：按逗号分割，但要处理类型中的括号和 COMMENT 中的引号

    # 使用正则分割，同时考虑括号和引号
    # 模式：匹配逗号，但不在括号或引号内
    column_definitions = re.split(r',(?![^(]*\))', columns_text)

    for col_def in column_definitions:
        col_def = col_def.strip()
        if not col_def:
            continue

        # 提取字段名和类型
        # 支持多种引号格式: `name`, "name", [name], name
        # 类型可以包含: VARCHAR(255), TEXT CHARACTER SET... COMMENT '...' 等

        # 首先尝试带反引号的字段名（MySQL 标准）
        match = re.match(r'^`([^`]+)`\s+(.+)$', col_def)
        if not match:
            # 尝试带双引号的字段名（PostgreSQL/DM）
            match = re.match(r'^"([^"]+)"\s+(.+)$', col_def)
        if not match:
            # 尝试带方括号的字段名（SQL Server）
            match = re.match(r'^\[([^\]]+)\]\s+(.+)$', col_def)
        if not match:
            # 尝试不带引号的字段名
            match = re.match(r'^([\w\u4e00-\u9fff/]+)\s+(.+)$', col_def)

        if match:
            column_name = match.group(1).strip()
            column_type_full = match.group(2).strip()

            # 如果有 COMMENT，提取类型部分（去掉 COMMENT）
            comment_match = re.match(r'^(.+?)\s+COMMENT\s+[\'"](.+)[\'"]\s*$', column_type_full, re.IGNORECASE)
            if comment_match:
                column_type = comment_match.group(1).strip()
                column_comment = comment_match.group(2).strip()
            else:
                column_type = column_type_full
                column_comment = None

            # 翻译字段名为英文（如果字段名包含中文）
            english_name = _translate_field_name(column_name)

            # 添加到完整字段信息列表
            result['columns'].append({
                'name': column_name,
                'type': column_type,
                'comment': column_comment,
                'english_name': english_name,
                'full_definition': col_def
            })

            # 添加字段名到单独的列表
            result['column_names'].append(column_name)

            # 添加字段名到类型的映射
            result['column_types'][column_name] = column_type

    # 5. 提取示例数据部分（在 /* */ 之间）
    if len(parts) > 1:
        # 直接保存原始的示例数据字符串
        result['sample_data_raw'] = parts[1].split('*/')[0].strip()

    return result


def parse_multiple_tables_schemas(schemas_dict: Dict[str, str]) -> Dict[str, Dict]:
    """
    解析多个表的 schema

    Args:
        schemas_dict: {表名: CREATE TABLE 语句} 的字典

    Returns:
        {表名: 解析结果} 的字典
    """
    results = {}
    for table_name, schema in schemas_dict.items():
        parsed = parse_table_schema(schema)
        results[table_name] = parsed


    return results


# 测试代码
if __name__ == '__main__':

    from langchain_community.utilities import SQLDatabase
    
    uri = f'mysql+mysqlconnector://root:liucd123@127.0.0.1:3306/12345'
    business_db = SQLDatabase.from_uri(uri, sample_rows_in_table_info=0)
    table_names = ['hongkou']

    # uri =  f'postgresql+pg8000://postgres:liucd123@127.0.0.1:5432/postgres'
    # business_db = SQLDatabase.from_uri(uri)
    # table_names = ['shanghai']

    test_schema = business_db.get_table_info(table_names=table_names, )
    print(test_schema)
    # print('=' * 100)
    # # 解析
    # parsed = parse_table_schema(test_schema)
    # print(parsed)

    print(_translate_field_name('中国'))