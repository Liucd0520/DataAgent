"""
Schema string 构建器模块

"""

from typing import Dict, List


def build_table_schema(
    table_name: str,
    columns: List[Dict[str, str]],
    table_comment: str = ""
) -> str:
    """
    根据结构化数据生成 MySQL CREATE TABLE 语句

    Args:
        table_name: 表名
        columns: 列定义列表，每个元素包含:
            - name: 字段名
            - type: 字段类型 (VARCHAR, BIGINT, TEXT, ENUM 等)
            - comment: 字段注释 (可选)
            - englishName: 英文字段名 (可选，用于记录)
        table_comment: 表注释

    Returns:
        str: 完整的 CREATE TABLE 语句

    Example:
        >>> columns = [
        ...     {"name": "id", "type": "BIGINT", "comment": "主键ID"},
        ...     {"name": "name", "type": "VARCHAR(255)", "comment": "名称"}
        ... ]
        >>> schema_str = build_create_table_sql("users", columns, "用户表")
        >>> print(schema_str)
    """

    # MySQL 使用反引号
    quote = "`"

    # 固定的 MySQL 配置
    collate = "utf8mb4_0900_ai_ci"
    engine = "InnoDB"
    charset = "utf8mb4"

    # 构建列定义部分
    column_definitions = []

    for col in columns:
        col_name = col.get("name", "")
        col_type = col.get("type", "")
        col_comment = col.get("comment", "")

        # 字段名加引号
        col_def = f"    {quote}{col_name}{quote} {col_type}"

        # 如果有注释，添加 COMMENT
        if col_comment:
            col_def += f" COMMENT '{col_comment}'"

        column_definitions.append(col_def)

    # 组合所有列定义
    columns_sql = ",\n".join(column_definitions)

    # 构建 CREATE TABLE 语句
    schema_str = f"CREATE TABLE {quote}{table_name}{quote} (\n{columns_sql}\n)"

    # 添加 MySQL 表级选项
    schema_str += f" DEFAULT CHARSET={charset} COLLATE={collate} ENGINE={engine}"

    # 添加表注释
    if table_comment:
        schema_str += f" COMMENT='{table_comment}'"

    return schema_str




if __name__ == "__main__":

    
    # 测试示例
    test_schema = {
        "table": {
            "name": "file_processing_records",
            "comment": "文件处理记录表",
            "columns": [
                {
                    "name": "processing_start_time",
                    "type": "TIMESTAMP",
                    "comment": "处理开始时间",
                    "englishName": ""
                },
                {
                    "name": "processing_end_time",
                    "type": "TIMESTAMP",
                    "comment": "处理结束时间",
                    "englishName": ""
                },
                {
                    "name": "file_name",
                    "type": "VARCHAR(255)",
                    "comment": "文件名",
                    "englishName": "fileName"
                },
                {
                    "name": "status",
                    "type": "ENUM('pending', 'processing', 'completed', 'failed')",
                    "comment": "处理状态",
                    "englishName": ""
                }
            ]
        }
    }

    table_info = test_schema.get("table", {})

    table_name = table_info.get("name", "")
    table_comment = table_info.get("comment", "")
    columns = table_info.get("columns", [])

    schema_str = build_table_schema(
        table_name=table_name,
        columns=columns,
        table_comment=table_comment
    )


    
    print(schema_str)
    print("\n" + "="*50 + "\n")
