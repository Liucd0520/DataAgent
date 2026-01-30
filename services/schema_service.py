"""
Schema 处理服务层

处理 schema 相关的业务逻辑，包括：
1. 枚举值处理
2. 表描述生成
3. 字段名翻译
"""

from typing import Dict, List, Any
from DataAgent.datasource.chain import table_descpt_chain, translate_chain
from DataAgent.datasource.schema_build import build_table_schema


def process_schema(schema_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    处理 schema 信息的主函数

    Args:
        schema_data: 包含 enumTopValues 和 table 的字典

    Returns:
        Dict[str, Any]: 处理后的 schema 数据
    """

    enum_top_values = schema_data.get("enumTopValues", [])
    table_info = schema_data.get("table", {})

    # 1. 处理枚举值，更新到 columns 的 comment 中
    columns =  _process_enum_values(enum_top_values, table_info.get("columns", []))

    # 2. 调用大模型生成表描述
    table_comment =  _generate_table_comment(table_info, columns)
    print('1', table_comment)
    # 3. 调用大模型翻译字段名
    columns =  _translate_column_names(columns)
    print('2', columns)
    # 构建返回结果
    result = {
        "table": {
            "name": table_info.get("name", ""),
            "comment": table_comment,
            "columns": columns
        },
        "enumTopValues": enum_top_values
    }

    return result


def _process_enum_values(
    enum_top_values: List[Dict[str, Any]],
    columns: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    将枚举值写入到对应字段的 comment 中

    逻辑：
    1. 保持原有注释不变
    2. 直接将新的枚举值信息追加到原注释后面

    Args:
        enum_top_values: 枚举值列表
        columns: 字段列表

    Returns:
        List[Dict[str, Any]]: 更新后的字段列表
    """

    # 创建字段名到枚举值的映射
    enum_map = {}
    for enum_item in enum_top_values:
        field_name = enum_item.get("name", "")
        values = enum_item.get("values", [])
        is_complete = enum_item.get("is_complete", True)

        enum_map[field_name] = {
            "values": values,
            "is_complete": is_complete
        }

    # 更新 columns 中的 comment
    for col in columns:
        col_name = col.get("name", "")

        # 如果该字段有枚举值，则更新 comment
        if col_name in enum_map:
            original_comment = col.get("comment", "")
            enum_info = enum_map[col_name]
            enum_values = enum_info["values"]
            is_complete = enum_info["is_complete"]

            # 格式化枚举值字符串
            enum_str = ', '.join([f"'{v}'" if v else 'NULL' for v in enum_values])

            # 根据 is_complete 标记来区分写法
            if is_complete:
                # 完整枚举
                enum_appendix = f"枚举类型，完整取值包括：[{enum_str}]"
            else:
                # 部分枚举，只显示了部分值
                enum_appendix = f"枚举类型，常见值包括：[{enum_str} ...]"

            # 保持原注释不变，直接追加枚举值信息
            if original_comment:
                enhanced_comment = f"{original_comment} {enum_appendix}"
            else:
                enhanced_comment = enum_appendix

            print(f"  ✓ 字段 '{col_name}': 添加 {len(enum_values)} 个枚举值（完整={'是' if is_complete else '否'}）")

            col["comment"] = enhanced_comment

    return columns


def _generate_table_comment(
    table_info: Dict[str, Any],
    columns: List[Dict[str, Any]]
) -> str:
    """
    调用大模型生成表描述

    Args:
        table_info: 表信息
        columns: 字段列表

    Returns:
        str: 生成的表描述
    """

    # 使用 schema_build.py 中的函数构建 schema 字符串
    table_name = table_info.get("name", "")
    existing_comment = table_info.get("comment", "")

    # 构建 schema 字符串
    schema_str = build_table_schema(
        table_name=table_name,
        columns=columns,
        table_comment=existing_comment
    )

    # 调用大模型生成表描述
    try:
        print('==> inter')
        response =  table_descpt_chain.invoke({
            "table_schema": schema_str
        })
        print('response: ', response)
        return response.content.strip() if hasattr(response, 'content') else str(response).strip()
    except Exception as e:
        # 如果大模型调用失败，返回原有注释
        print(f"Error generating table comment: {e}")
        return existing_comment


def _translate_column_names(columns: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    调用大模型批量翻译字段名为英文

    Args:
        columns: 字段列表

    Returns:
        List[Dict[str, Any]]: 更新了英文字段名的列表
    """

    # 收集需要翻译的字段名及其索引
    need_translate = []
    for idx, col in enumerate(columns):
        # 如果已经有 englishName，跳过
        if col.get("englishName"):
            continue

        field_name = col.get("name", "")
        if field_name:
            need_translate.append({
                "index": idx,
                "field_name": field_name
            })

    # 如果没有需要翻译的字段，直接返回
    if not need_translate:
        return columns

    try:
        # 构建批量输入
        inputs = [
            {"field_name": item["field_name"]}
            for item in need_translate
        ]

        # 使用 batch 方法批量翻译
        results = translate_chain.batch(inputs)

        # 将翻译结果映射回对应的列
        for item, response in zip(need_translate, results):
            idx = item["index"]
            col = columns[idx]

            # 提取翻译结果
            translated_name = response.content.strip() if hasattr(response, 'content') else str(response).strip()

            # 清理可能的花括号等标记
            translated_name = translated_name.strip("{}'\"")

            col["englishName"] = translated_name

    except Exception as e:
        print(f"Error batch translating field names: {e}")
        # 批量翻译失败时，将所有未翻译的字段设为空
        for item in need_translate:
            idx = item["index"]
            columns[idx]["englishName"] = ""

    return columns
