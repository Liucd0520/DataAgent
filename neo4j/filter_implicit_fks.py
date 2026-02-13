#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
筛选隐式外键关系
保留符合业务逻辑的高质量关系
"""

import json
import sys
from typing import List, Dict


def filter_implicit_foreign_keys(
    relationships: List[Dict],
    min_coverage: float = 0.85,
    max_null_ratio: float = 0.5,
    max_cardinality_ratio: float = 1.2,
    min_name_similarity: float = 0.3
) -> List[Dict]:
    """
    筛选隐式外键关系

    Args:
        relationships: 原始关系列表
        min_coverage: 最小覆盖率 (默认0.85)
        max_null_ratio: 最大空值率 (默认0.5)
        max_cardinality_ratio: 最大基数比 (默认1.2，允许稍微超过1)
        min_name_similarity: 最小命名相似度 (默认0.3)

    Returns:
        筛选后的关系列表
    """

    filtered = []

    for rel in relationships:
        # 基础指标
        coverage = rel.get('coverage', 0.0)
        null_ratio = rel.get('null_ratio', 1.0)
        card_ratio = rel.get('cardinality_ratio', 0.0)
        name_sim = rel.get('name_similarity', 0.0)


        # 规则1: 覆盖率必须达标
        if coverage < min_coverage:
            continue

        # 规则2: 空值率不能过高
        if null_ratio > max_null_ratio:
            continue

        # 规则3: 基数比检查 (外键基数不应该超过主键太多)
        # 如果基数比 > 1.2，可能是误报
        if card_ratio > max_cardinality_ratio:
            # 例外：如果覆盖率是100%，且命名相似度高，可以保留
            if coverage < 1.0 or name_sim < 0.5:
                continue

        # 规则4: 命名相似度或业务逻辑检查
        # 如果命名相似度太低，需要极高的覆盖率才能通过
        if name_sim < min_name_similarity:
            # 相似度低但覆盖率>0.95，可能是真实关系
            if coverage < 0.95:
                continue

        # 额外过滤：排除id到id的误报（除非命名明显相关）
        if rel['fk_column'].lower() == 'id' and rel['pk_column'].lower() == 'id':
            # 如果两个表名有明显关联才保留
            fk_table = rel['fk_table'].lower()
            pk_table = rel['pk_table'].lower()
            # 检查表名是否有包含关系
            if not (fk_table in pk_table or pk_table in fk_table or
                    '_'.join(fk_table.split('_')[:-1]) in pk_table):
                # 覆盖率必须是100%才考虑
                if coverage < 1.0:
                    continue

        # 通过所有筛选
        filtered.append(rel)

    return filtered


def is_generic_id_column(col_name: str) -> bool:
    """
    判断是否为通用ID字段（容易误判的字段）
    """
    generic_patterns = ['id', 'ID', 'Id', 'key', 'KEY', 'Key']
    return col_name in generic_patterns


def has_table_name_relationship(fk_table: str, pk_table: str, fk_column: str, pk_column: str = None) -> bool:
    """
    判断两个表是否具有合理的命名关联关系
    """
    fk_table_lower = fk_table.lower()
    pk_table_lower = pk_table.lower()
    fk_column_lower = fk_column.lower()

    # 如果提供了主键列名，检查字段后缀是否匹配
    # 只有后缀相同才能有关系（例如 AUTHORITY_TYPE 和 AUTHORITY_ID 的后缀不同，不应该有关系）
    if pk_column:
        pk_column_lower = pk_column.lower()

        # 提取两个字段的后缀
        fk_suffix = fk_column_lower.split('_')[-1] if '_' in fk_column_lower else None
        pk_suffix = pk_column_lower.split('_')[-1] if '_' in pk_column_lower else None

        # 如果两个字段都有下划线后缀
        if fk_suffix and pk_suffix:
            # 后缀不同，直接返回 False
            if fk_suffix != pk_suffix:
                return False

        # 字段名完全相同，认为是有效关系
        if fk_column_lower == pk_column_lower:
            return True

    # 1. 从外键列名提取可能的表名
    if fk_column_lower.endswith('_id'):
        potential_table = fk_column_lower[:-3]
    elif fk_column_lower.endswith('_key'):
        potential_table = fk_column_lower[:-4]
    else:
        # 对于非_id/_key字段，尝试从列名提取业务实体
        # 例如: CUSTOMER_NAME -> customer
        parts = fk_column_lower.split('_')
        if len(parts) > 1:
            # 取第一部分作为可能的表名
            potential_table = parts[0]
        else:
            potential_table = fk_column_lower

    # 2. 检查表名是否匹配
    # 完全匹配
    if potential_table == pk_table_lower:
        return True

    # 包含关系
    if potential_table in pk_table_lower or pk_table_lower in potential_table:
        return True

    # 3. 中间表检查 (例如: framework_role_authority -> framework_role)
    if '_' in fk_table_lower:
        # 分割外键表名
        fk_parts = fk_table_lower.split('_')

        # 检查主表名是否是外键表名的一部分
        if pk_table_lower in fk_parts or any(pk_table_lower in part for part in fk_parts):
            return True

        # 检查外键列名是否指向主表
        if fk_column_lower.endswith('_id'):
            col_base = fk_column_lower[:-3]
            if col_base in pk_table_lower or pk_table_lower == col_base:
                return True
        else:
            # 对于非_id字段，检查列名第一部分是否与主表相关
            col_parts = fk_column_lower.split('_')
            if col_parts and col_parts[0] in pk_table_lower:
                return True

    return False


def advanced_filter(relationships: List[Dict]) -> List[Dict]:
    """
    高级筛选：基于业务逻辑和规则
    """
    filtered = []

    for rel in relationships:
        coverage = rel['coverage']
        null_ratio = rel['null_ratio']
        card_ratio = rel['cardinality_ratio']
        name_sim = rel['name_similarity']

        # 获取字段信息
        fk_col = rel['fk_column'].lower()
        pk_col = rel['pk_column'].lower()
        fk_table = rel['fk_table'].lower()
        pk_table = rel['pk_table'].lower()

        # === 严格过滤通用ID字段的误报 ===
        # 如果是 id -> id 或 ID -> ID 这样的关系，必须满足严格的表名关联
        if (is_generic_id_column(rel['fk_column']) and
            is_generic_id_column(rel['pk_column'])):

            # 必须有明确的表名关联
            if not has_table_name_relationship(rel['fk_table'], rel['pk_table'], rel['fk_column'], rel['pk_column']):
                # 排除：通用ID字段且无表名关联
                continue

            # 即使有表名关联，也要求更高的覆盖率
            if coverage < 0.95 or null_ratio > 0.1:
                continue

            # 通过严格检查，保留
            filtered.append(rel)
            continue

        # === 高质量关系（直接保留） ===
        if (coverage >= 0.95 and
            null_ratio <= 0.1 and
            name_sim >= 0.5):
            filtered.append(rel)
            continue

        # === 命名完全匹配的关系 ===
        # 例如: customer_id -> customer.CUSTOMER_ID 或 role_id -> role.ID
        # 或: CUSTOMER_NAME -> CUSTOMER_NAME
        if fk_col.endswith('_id') or fk_col.endswith('_key'):
            # 提取可能的表名
            if fk_col.endswith('_id'):
                potential_table = fk_col[:-3]
            else:
                potential_table = fk_col[:-4]

            # 检查是否与主表匹配
            if has_table_name_relationship(rel['fk_table'], rel['pk_table'], rel['fk_column'], rel['pk_column']):
                if coverage >= 0.85 and null_ratio <= 0.3:
                    filtered.append(rel)
                    continue

        # === 完全匹配的字段名（非_id/_key后缀） ===
        # 例如: CUSTOMER_NAME -> CUSTOMER_NAME
        if fk_col == pk_col:
            # 字段名完全相同，可能是有效的关系
            if coverage >= 0.85 and null_ratio <= 0.3:
                filtered.append(rel)
                continue

        # === 单边通用ID的关系（一边是通用ID，另一边是具体字段名）===
        # 例如: user_role.role_id -> role.ID
        if (is_generic_id_column(rel['pk_column']) and
            not is_generic_id_column(rel['fk_column'])):

            # 外键字段名必须与主表名相关
            if has_table_name_relationship(rel['fk_table'], rel['pk_table'], rel['fk_column'], rel['pk_column']):
                if coverage >= 0.85 and null_ratio <= 0.3:
                    filtered.append(rel)
                    continue

        # === 中间表多对多关系 ===
        # 例如: framework_role_authority.ROLE_ID -> framework_role.ID
        if '_' in fk_table:
            # 提取表名各部分
            table_parts = fk_table.split('_')

            # 检查外键列是否匹配主表
            if fk_col.endswith('_id'):
                col_table_name = fk_col[:-3]

                # 检查列名指向的表是否在主表中
                if (col_table_name in pk_table or
                    pk_table in col_table_name or
                    col_table_name in table_parts):
                    if coverage >= 0.85 and null_ratio <= 0.3:
                        filtered.append(rel)
                        continue

        # === 高覆盖率低空值率 ===
        if coverage >= 0.95 and null_ratio <= 0.05:
            # 检查基数比是否合理
            if card_ratio <= 1.0:
                filtered.append(rel)
                continue

        # === 特殊业务模式 ===

        # 1. status/state字段 (通常是多对一)
        if 'status' in fk_col or 'state' in fk_col:
            # 检查表名是否相关（例如 order.status_id -> order_status）
            if ('status' in pk_table or 'state' in pk_table):
                if coverage >= 0.85 and null_ratio <= 0.1:
                    filtered.append(rel)
                    continue

        # 2. type字段
        if 'type' in fk_col and '_type' in fk_col:
            if coverage >= 0.85 and null_ratio <= 0.1:
                filtered.append(rel)
                continue

        # === 明显的关联关系 (表名包含) ===
        # 例如: ne_service_item.ne_id -> ne.NE_ID
        if (fk_col in pk_col or pk_col in fk_col):
            if has_table_name_relationship(rel['fk_table'], rel['pk_table'], rel['fk_column'], rel['pk_column']):
                if coverage >= 0.9:
                    filtered.append(rel)
                    continue

    return filtered


def categorize_relationships(relationships: List[Dict]) -> Dict[str, List[Dict]]:
    """
    将关系分类
    """
    categories = {
        'high_quality': [],      # 高质量：覆盖率高、空值率低、命名匹配
        'medium_quality': [],    # 中等质量：覆盖率尚可
        'low_quality': [],       # 低质量：可能误报
        'suspicious': []         # 可疑关系
    }

    for rel in relationships:
        coverage = rel['coverage']
        null_ratio = rel['null_ratio']
        name_sim = rel['name_similarity']
        card_ratio = rel['cardinality_ratio']
        fk_col = rel['fk_column'].lower()
        pk_col = rel['pk_column'].lower()
        fk_type = rel['fk_type']
        pk_type = rel['pk_type']

        # 可疑：通用ID字段且无表名关联
        if (is_generic_id_column(rel['fk_column']) and
            is_generic_id_column(rel['pk_column']) and
            not has_table_name_relationship(rel['fk_table'], rel['pk_table'], rel['fk_column'], rel['pk_column'])):
            categories['suspicious'].append(rel)
            continue

        # 可疑：ID到ID但命名相似度极低
        if (is_generic_id_column(rel['fk_column']) and
            is_generic_id_column(rel['pk_column']) and
            name_sim < 0.3):
            categories['suspicious'].append(rel)
            continue

        # 高质量：覆盖率非常高且空值率低，且有明确的命名关联
        if (coverage >= 0.95 and
            null_ratio <= 0.1 and
            has_table_name_relationship(rel['fk_table'], rel['pk_table'], rel['fk_column'], rel['pk_column'])):
            categories['high_quality'].append(rel)
            continue

        '''# 高质量：命名非常匹配
        if (coverage >= 0.90 and
            null_ratio <= 0.15 and
            name_sim >= 0.7):
            categories['high_quality'].append(rel)
            continue

        # 中等质量：基本达标
        elif coverage >= 0.85 and null_ratio <= 0.3:
            categories['medium_quality'].append(rel)'''

        # 高质量：字段名完全匹配（例如 CUSTOMER_NAME -> CUSTOMER_NAME）
        if fk_col == pk_col and coverage >= 0.85 and null_ratio <= 0.3:
            categories['high_quality'].append(rel)
            continue

        # 高质量：足够匹配
        if coverage >= 0.85 and null_ratio <= 0.15 and (card_ratio < 0.1 and name_sim > 0.5 or name_sim == 1.0) and (fk_type == pk_type):
            categories['high_quality'].append(rel)
            continue

        # 低质量
        else:
            categories['low_quality'].append(rel)

    return categories


def main():
    """主函数"""
    input_file = '/data/liyiru/mysql-graph/implicit_foreign_keys.json'

    # 读取原始数据
    print(f"读取文件: {input_file}")
    with open(input_file, 'r', encoding='utf-8') as f:
        relationships = json.load(f)

    print(f"\n原始关系数量: {len(relationships)}")

    # 基础筛选
    print("\n=== 基础筛选 ===")
    filtered_basic = filter_implicit_foreign_keys(
        relationships,
        min_coverage=0.85,
        max_null_ratio=0.5,
        max_cardinality_ratio=1.2,
        min_name_similarity=0.3
    )
    print(f"基础筛选后: {len(filtered_basic)} 个关系")

    # 高级筛选
    print("\n=== 高级筛选 ===")
    filtered_advanced = advanced_filter(relationships)
    print(f"高级筛选后: {len(filtered_advanced)} 个关系")

    # 分类
    print("\n=== 关系分类 ===")
    categories = categorize_relationships(filtered_advanced)

    # 输出分类统计
    for category, rels in categories.items():
        print(f"{category}: {len(rels)} 个关系")

    # 保存筛选结果
    output_files = {
        'filtered_basic': '/data/liyiru/mysql-graph/implicit_fks_filtered_basic.json',
        'filtered_advanced': '/data/liyiru/mysql-graph/implicit_fks_filtered_advanced.json',
        'high_quality': '/data/liyiru/mysql-graph/implicit_fks_high_quality.json',
        'medium_quality': '/data/liyiru/mysql-graph/implicit_fks_medium_quality.json'
    }

    # 保存基础筛选结果
    with open(output_files['filtered_basic'], 'w', encoding='utf-8') as f:
        json.dump(filtered_basic, f, indent=2, ensure_ascii=False)
    print(f"\n已保存基础筛选结果到: {output_files['filtered_basic']}")

    # 保存高级筛选结果
    with open(output_files['filtered_advanced'], 'w', encoding='utf-8') as f:
        json.dump(filtered_advanced, f, indent=2, ensure_ascii=False)
    print(f"已保存高级筛选结果到: {output_files['filtered_advanced']}")

    # 保存高质量关系
    with open(output_files['high_quality'], 'w', encoding='utf-8') as f:
        json.dump(categories['high_quality'], f, indent=2, ensure_ascii=False)
    print(f"已保存高质量关系到: {output_files['high_quality']}")

    # 保存中等质量关系
    with open(output_files['medium_quality'], 'w', encoding='utf-8') as f:
        json.dump(categories['medium_quality'], f, indent=2, ensure_ascii=False)
    print(f"已保存中等质量关系到: {output_files['medium_quality']}")

    # 打印示例
    print("\n=== 高质量关系示例 (前10个) ===")
    for i, rel in enumerate(categories['high_quality'][:10], 1):
        print(f"{i}. {rel['fk_table']}.{rel['fk_column']} -> {rel['pk_table']}.{rel['pk_column']}")
        print(f"   覆盖率: {rel['coverage']:.2%} | "
              f"空值率: {rel['null_ratio']:.2%} | "
              f"基数比: {rel['cardinality_ratio']:.2f} | "
              f"相似度: {rel['name_similarity']:.2f}")
        print()

    return filtered_advanced


if __name__ == "__main__":
    main()
