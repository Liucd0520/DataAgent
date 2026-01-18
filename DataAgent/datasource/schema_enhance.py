"""
增强schema：
1. 枚举值类型的列增加TOP10样例值
2. 表描述的重新生成

"""
import sys
from pathlib import Path
# 将项目根目录添加到 Python 路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from DataAgent.datasource.chain import table_descpt_chain
from typing import Dict, List
import re
from langchain_community.utilities import SQLDatabase

def schema_enum_enhance(parsed_schemas_by_table: Dict[str, Dict], business_db: SQLDatabase) -> Dict[str, Dict]:
    """
    增强表的 schema：为字符串类型的枚举字段添加 TOP10 枚举值到描述中

    Args:
        parsed_schemas_by_table: {表名: 解析后的表信息字典}，包含 columns, column_names, column_types 等
        business_db: SQLDatabase 实例，用于查询数据库获取枚举值

    Returns:
        增强后的表信息字典，格式与输入相同，但字段的 comment 被追加了枚举值信息
    """
    enhanced_schemas = {}

    for table_name, table_info in parsed_schemas_by_table.items():
        print(f"正在处理表: {table_name}")

        # 一次性获取该表所有字符串字段的枚举值
        columns_enum_values = _get_table_enum_values_batch(business_db, table_name, table_info['columns'])

        # 复制原始表信息
        enhanced_table = {
            'table_name': table_info['table_name'],
            'table_comment': table_info['table_comment'],
            'columns': [],
            'column_names': list(table_info['column_names']),
            'column_types': dict(table_info['column_types']),
            'sample_data_raw': table_info['sample_data_raw']
        }

        # 遍历每个字段
        for column in table_info['columns']:
            column_name = column['name']
            column_type = column['type']
            original_comment = column['comment']

            # 检测是否为字符串类型（TEXT, VARCHAR, CHAR 等）
            # 支持 MySQL: CHAR, VARCHAR(255), TEXT, TINYTEXT, MEDIUMTEXT, LONGTEXT
            # 支持 PostgreSQL: CHAR, VARCHAR, TEXT, CHARACTER VARYING
            # 支持 达梦(DM): CHAR, VARCHAR(255), TEXT, CLOB, LONGVARCHAR, CHARACTER VARYING
            if re.match(r'^(TEXT|VARCHAR\(\d+\)|CHAR\(\d+\)|CHAR|VARCHAR|LONGTEXT|MEDIUMTEXT|TINYTEXT|CHARACTER VARYING|CLOB|LONGVARCHAR)', column_type, re.IGNORECASE):
                # 从批量查询结果中获取枚举值
                enum_info = columns_enum_values.get(column_name)

                if enum_info:
                    new_enum_values = enum_info['values']
                    total_count = enum_info['total_count']
                    is_complete = enum_info.get('is_complete', True)  # 默认为完整，兼容旧数据

                    # 从原注释中提取已存在的枚举值
                    existing_enums = _extract_enum_values_from_comment(original_comment)

                    # 合并新旧枚举值并去重（保持顺序）
                    merged_enums = list(dict.fromkeys(list(existing_enums) + new_enum_values))

                    # 构建新的注释
                    base_comment = _remove_enum_part_from_comment(original_comment)
                    enum_str = ', '.join([f"'{v}'" if v else 'NULL' for v in merged_enums])

                    # 根据 is_complete 标记来区分写法
                    if is_complete:
                        # 完整枚举
                        enhanced_comment = f"{base_comment} 枚举类型，完整取值包括：[{enum_str}]".strip()
                    else:
                        # 部分枚举，只显示了部分值（不显示具体数量，因为可能不准确）
                        enhanced_comment = f"{base_comment} 枚举类型，常见值包括：[{enum_str} ...]".strip()

                    added_count = len(set(new_enum_values) - existing_enums)
                    print(f"  ✓ 字段 '{column_name}': 原有 {len(existing_enums)} 个枚举值，新增 {added_count} 个，共 {len(merged_enums)} 个（总共 {total_count} 个不重复值，完整={'是' if is_complete else '否'}）")
                else:
                    enhanced_comment = original_comment
                    print(f"  - 字段 '{column_name}': 无明显枚举值（唯一值过多或无数据）")
            else:
                enhanced_comment = original_comment

            # 添加增强后的字段信息
            enhanced_table['columns'].append({
                'name': column_name,
                'type': column_type,
                'comment': enhanced_comment,
                'full_definition': column['full_definition']
            })

        enhanced_schemas[table_name] = enhanced_table
        print(f"完成表 '{table_name}' 的枚举值增强\n")

    return enhanced_schemas


def _extract_enum_values_from_comment(comment: str | None) -> set:
    """
    从注释中提取已存在的枚举值

    Args:
        comment: 字段注释字符串

    Returns:
        提取到的枚举值集合
    """
    if not comment:
        return set()

    # 匹配单引号中的值 'xxx'
    single_quotes = re.findall(r"'([^']*)'", comment)

    # 匹配双引号中的值 "xxx"
    double_quotes = re.findall(r'"([^"]*)"', comment)

    # 匹配冒号后的逗号分隔值（如 "状态: active,inactive"）
    colon_vals = []
    if ':' in comment:
        after_colon = comment.split(':', 1)[1]
        colon_vals = [x.strip() for x in after_colon.split(',') if x.strip()]

    return set(single_quotes + double_quotes + colon_vals)


def _remove_enum_part_from_comment(comment: str | None) -> str:
    """
    从注释中移除枚举值部分，保留基础注释

    Args:
        comment: 字段注释字符串

    Returns:
        移除枚举值部分后的注释
    """
    if not comment:
        return ''

    # 移除 [枚举值: ...] 格式的部分
    # 匹配模式：[枚举值: xxx]
    cleaned = re.sub(r'\s*\[枚举值:\s*[^\]]*\]\s*$', '', comment)

    return cleaned.strip()


def _get_table_enum_values_batch(business_db: SQLDatabase, table_name: str, columns: List[Dict], sample_rows: int = 10000, top_n: int = 10, max_distinct_threshold: int = 100) -> Dict[str, Dict]:
    """
    批量获取表中所有字符串字段的枚举值

    策略：
    1. 筛选出所有字符串类型的字段
    2. 从表中采样 10000 行数据
    3. 一次性统计所有字符串字段的不重复值数量（使用子查询 + COUNT(DISTINCT)）
    4. 对不重复值 <= max_distinct_threshold 的字段，获取前 top_n 个最常见的值

    Args:
        business_db: SQLDatabase 实例
        table_name: 表名
        columns: 字段信息列表
        sample_rows: 采样的行数（默认 10000）
        top_n: 返回的枚举值数量（默认 10）
        max_distinct_threshold: 判断是否为枚举类型的最大不重复值数量（默认 100）

    Returns:
        {字段名: {'values': 枚举值列表, 'total_count': 实际不重复值总数}} 的字典
    """
    try:
        # 检测数据库方言，选择合适的引号符
        # MySQL: `backtick`, PostgreSQL/DM: "double quote
        dialect_name = business_db.dialect
        
        if dialect_name in ['postgresql', 'postgres', 'dm', 'dameng']:
            quote = '"'  # PostgreSQL 和达梦使用双引号
        elif dialect_name == 'mysql':
            quote = '`'  # MySQL 使用反引号
        else:
            quote = '`'  # 默认使用 MySQL 风格

        # 筛选出字符串类型的字段
        column_names = []
        for column in columns:
            print('-------->', column)
            column_name = column['name']
            column_type = column['type']

            # 检测是否为字符串类型
            if re.match(r'^(TEXT|VARCHAR\(\d+\)|CHAR\(\d+\)|CHAR|VARCHAR|LONGTEXT|MEDIUMTEXT|TINYTEXT|CHARACTER VARYING|CLOB|LONGVARCHAR)', column_type, re.IGNORECASE):
                column_names.append(column_name)

        if not column_names:
            return {}

        # 步骤1：一次性统计所有字段的不重复值数量
        distinct_counts_query = f"""
        SELECT {', '.join([f'COUNT(DISTINCT {quote}{col}{quote}) AS {quote}{col}{quote}' for col in column_names])}
        FROM (
            SELECT {', '.join([f'{quote}{col}{quote}' for col in column_names])}
            FROM {quote}{table_name}{quote}
            LIMIT {sample_rows}
        ) AS sampled_data
        """
        print(distinct_counts_query)

        distinct_result = business_db.run(distinct_counts_query, include_columns=True)
        print(distinct_result)

        # 解析不重复值数量
        # 返回格式: [{'col1': 10, 'col2': 20}]
        import ast
        distinct_counts = {}
        if distinct_result and len(distinct_result) > 0:
            # 使用 ast.literal_eval 直接解析字典列表
            try:
                result_dict = ast.literal_eval(distinct_result)[0]
                if isinstance(result_dict, dict):
                    distinct_counts = result_dict
                    print(f"解析到的 distinct_counts: {distinct_counts}")
            except (ValueError, SyntaxError) as e:
                # 降级：如果解析失败，逐个字段查询
                print(f"解析 distinct_result 失败: {e}")
                pass

        # 步骤2：筛选出需要枚举值的字段（不重复值 <= max_distinct_threshold）
        # 注意：distinct_counts 为 0 说明采样数据全是 NULL，但是真实情况未必是NULL，可能是取sample_rows不够造成的
        candidate_columns = [col for col in column_names if distinct_counts.get(col, 999) <= max_distinct_threshold]
        print(candidate_columns)

        if not candidate_columns:
            return {}

        # 步骤3：获取候选字段的枚举值（按频率排序）
        columns_enum_values = {}

        # 对每个候选字段，查询其最常见的值
        for column_name in candidate_columns:
            # 查询最常见的 top_n 个值
            top_values_query = f"""
            SELECT {quote}{column_name}{quote}, COUNT(*) as count
            FROM (
                SELECT {quote}{column_name}{quote}
                FROM {quote}{table_name}{quote}
                WHERE {quote}{column_name}{quote} IS NOT NULL
                LIMIT {sample_rows}
            ) AS sampled_col
            GROUP BY {quote}{column_name}{quote}
            ORDER BY count DESC
            LIMIT {top_n}
            """

            top_values_result = business_db.run(top_values_query, include_columns=True)
            print(top_values_result)

            # 解析结果 - 获取枚举值
            # 返回格式: [{'热线二级': '住房保障', 'count': 2953}, ...]
            enum_values = []
            import ast
            try:
                result_list = ast.literal_eval(top_values_result)
                for row_dict in result_list:
                    if isinstance(row_dict, dict) and column_name in row_dict:
                        value = row_dict[column_name]
                        if value and value != 'NULL':
                            enum_values.append(value)
            except (ValueError, SyntaxError):
                pass

            if enum_values:
                # 获取该字段的实际不重复值总数
                distinct_count = distinct_counts.get(column_name, 0)

                # 判断是否达到了 LIMIT（可能还有更多值）
                # 1. distinct_count == 0 且 len(enum_values) == top_n：说明采样全NULL，但查到了top_n个，可能还有更多
                # 2. distinct_count == len(enum_values) 且 len(enum_values) == top_n：说明恰好有top_n个，是完整的
                # 3. distinct_count > len(enum_values)：说明还有更多值没显示出来
                # 4. distinct_count == len(enum_values) 且 len(enum_values) < top_n：说明这就是全部了

                if distinct_count == 0:
                    # 采样数据全NULL，直接用获取到的数量
                    # 但如果达到了 top_n，说明可能还有更多
                    total_count = len(enum_values)
                    is_complete = (len(enum_values) < top_n)
                elif len(enum_values) >= top_n:
                    # 达到了 LIMIT，说明可能还有更多（除非 distinct_count 恰好等于 top_n）
                    total_count = distinct_count
                    is_complete = (distinct_count == len(enum_values))
                else:
                    # 没达到 LIMIT，说明获取到的就是全部
                    total_count = distinct_count
                    is_complete = True

                # 调试：打印获取 total_count 的过程
                print(f"字段 '{column_name}': distinct_count={distinct_count}, enum_values={len(enum_values)}, total_count={total_count}, is_complete={is_complete}")

                columns_enum_values[column_name] = {
                    'values': enum_values,
                    'total_count': total_count,
                    'is_complete': is_complete  # 标记是否为完整枚举
                }

        return columns_enum_values

    except Exception as e:
        print(f"    ⚠ 批量查询表 '{table_name}' 的枚举值时出错: {str(e)}")
        return {}





def schema_table_description_enhance(raw_schemas_by_table: Dict[str, str]) -> Dict[str, str]:
    """
    使用 LangChain 的 batch() 方法批量调用大模型生成表描述

    Args:
        raw_schemas_by_table: {表名: CREATE TABLE 语句字符串}

    Returns:
        {表名: 大模型生成的表描述}
    """
    # 提取表名列表和对应的 schema 列表
    table_names = list(raw_schemas_by_table.keys())
    schemas_list = list(raw_schemas_by_table.values())

    # 使用 batch() 方法批量调用大模型
    generated_descriptions_list = table_descpt_chain.batch(schemas_list)

    # 将结果映射回表名
    generated_descriptions = {}
    for table_name, generated_desc in zip(table_names, generated_descriptions_list):
        generated_descriptions[table_name] = generated_desc.content
        print(f"✓ 已完成表 '{table_name}' 的描述生成")
        print(f"  生成描述: {generated_desc.content}\n")

    return generated_descriptions


if __name__ == '__main__':
    


    from DataAgent.datasource.schema_obtain import schema_obtain
    uri = f'mysql+mysqlconnector://root:liucd123@127.0.0.1:3306/12345'
    business_db = SQLDatabase.from_uri(uri)
    table_names = ['pudong', 'shanghai', 'hongkou']
    raw_schemas_by_table, parsed_schemas_by_table = schema_obtain(business_db, table_names)
    
    # 测试表描述生成
    # generated_descriptions = schema_table_description_enhance(raw_schemas_by_table)

    # 测试枚举值获取
    # columns_enum_values = _get_table_enum_values_batch(business_db, 
    #                              table_names[0], 
    #                              columns=[{"name": "热线一级", "type": "text",}, 
    #                                       {"name": "热线二级", "type": "text",}]
    #                              )
    # print(columns_enum_values)

    # 测试schema增强功能
    res = schema_enum_enhance(parsed_schemas_by_table, business_db)
    print(res['shanghai']['columns'])