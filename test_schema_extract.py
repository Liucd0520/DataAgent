import re
from typing import Dict

def extract_table_schemas_debug(table_schemas: str) -> Dict[str, str]:
    """带调试信息的版本"""
    print(f"输入字符串长度: {len(table_schemas)}")
    print(f"输入前100字符: {repr(table_schemas[:100])}")

    # 正则表达式匹配 CREATE TABLE 语句，提取表名
    pattern = r'CREATE\s+TABLE\s+(?:[\w"]+\.)?[\["]?([\w"]+)[\"]]?\s*\('

    table_dict = {}
    # 找到所有 CREATE TABLE 的位置和表名
    create_table_positions = []
    for match in re.finditer(pattern, table_schemas, re.IGNORECASE):
        table_name = match.group(1).strip('"')
        create_table_positions.append((match.start(), table_name))
        print(f"找到表: {table_name} at position {match.start()}")

    print(f"\n总共找到 {len(create_table_positions)} 个表")

    # 基于位置提取每个表的完整 schema
    for i, (start_pos, table_name) in enumerate(create_table_positions):
        print(f"\n处理表 {i+1}: {table_name}, start_pos={start_pos}")

        # 如果不是最后一个表，找到下一个 CREATE TABLE 前的 \n\n
        if i < len(create_table_positions) - 1:
            next_start = create_table_positions[i + 1][0]
            print(f"  下一个表位置: {next_start}")

            # 从下一个 CREATE TABLE 向前查找 \n\n
            end_pos = next_start
            while end_pos > start_pos and end_pos - 2 >= start_pos:
                if table_schemas[end_pos - 2:end_pos] == '\n\n':
                    print(f"  找到分隔符 \\n\\n at position {end_pos-2}")
                    break
                end_pos -= 1
            print(f"  结束位置: {end_pos}")
        else:
            # 最后一个表，直接到末尾
            end_pos = len(table_schemas)
            print(f"  最后一个表，结束位置: {end_pos}")

        # 提取并清理首尾空白
        full_statement = table_schemas[start_pos:end_pos].strip()
        print(f"  提取长度: {len(full_statement)}")
        table_dict[table_name] = full_statement

    return table_dict


# 测试数据
test_schema = """

CREATE TABLE hongkou (
        `案件编号` BIGINT,
        `原编号` BIGINT
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_0900_ai_ci

/*
3 rows from hongkou table:
案件编号        原编号
202408317745200116      20240831030597
*/


CREATE TABLE pudong (
        `状态` TEXT,
        `工单号` TEXT
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_0900_ai_ci

/*
3 rows from pudong table:
状态    工单号
已结案  2209P2025174
*/


CREATE TABLE shanghai (
        `工单编号` BIGINT,
        `工号` BIGINT
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_0900_ai_ci

/*
3 rows from shanghai table:
工单编号        工号
20240101000072  3830
*/
"""

print("=" * 60)
result = extract_table_schemas_debug(test_schema)
print("=" * 60)
print(f"\n最终结果: {list(result.keys())}")
for table_name, schema in result.items():
    print(f"\n表名: {table_name}")
    print(f"Schema 长度: {len(schema)} 字符")
