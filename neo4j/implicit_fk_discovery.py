#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
隐式外键发现算法
基于三层级规则体系：元数据过滤 -> 数值分布分析 -> 图论优化
"""

import json
import re
from typing import List, Dict, Tuple, Set
from collections import defaultdict
import mysql.connector
from mysql.connector import Error
from difflib import SequenceMatcher
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ImplicitFKDiscoverer:
    """隐式外键发现器"""

    def __init__(self, host, port, user, password, database):
        """初始化数据库连接"""
        self.config = {
            'host': host,
            'port': port,
            'user': user,
            'password': password,
            'database': database
        }
        self.conn = None
        self.cursor = None

    def connect(self):
        """建立数据库连接"""
        try:
            self.conn = mysql.connector.connect(**self.config)
            self.cursor = self.conn.cursor()
            logger.info(f"成功连接到数据库: {self.config['database']}")
            return True
        except Error as e:
            logger.error(f"数据库连接失败: {e}")
            return False

    def close(self):
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def get_all_tables(self) -> List[str]:
        """获取所有表名"""
        self.cursor.execute("SHOW TABLES")
        tables = [table[0] for table in self.cursor.fetchall()]
        logger.info(f"发现 {len(tables)} 个表")
        return tables

    def get_table_columns(self, table_name: str) -> List[Dict]:
        """获取表的所有列信息"""
        query = """
            SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE,
                   IS_NULLABLE, COLUMN_KEY, COLUMN_DEFAULT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
        """
        self.cursor.execute(query, (self.config['database'], table_name))
        columns = []
        for row in self.cursor.fetchall():
            columns.append({
                'name': row[0],
                'data_type': row[1],
                'column_type': row[2],
                'is_nullable': row[3],
                'column_key': row[4],  # PRI, UNI, MUL, or empty
                'default': row[5]
            })
        return columns

    def get_primary_keys(self, table_name: str) -> List[str]:
        """获取表的主键列"""
        query = """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            AND CONSTRAINT_NAME = 'PRIMARY'
            ORDER BY ORDINAL_POSITION
        """
        self.cursor.execute(query, (self.config['database'], table_name))
        return [row[0] for row in self.cursor.fetchall()]

    def get_column_sample_values(self, table_name: str, column_name: str,
                                 limit: int = 1000) -> Set:
        """获取列的样本值（用于包含依赖分析）"""
        query = f"SELECT DISTINCT `{column_name}` FROM `{table_name}` WHERE `{column_name}` IS NOT NULL LIMIT {limit}"
        try:
            self.cursor.execute(query)
            values = set(row[0] for row in self.cursor.fetchall() if row[0] is not None)
            return values
        except Error as e:
            logger.warning(f"获取 {table_name}.{column_name} 样本值失败: {e}")
            return set()

    def get_column_stats(self, table_name: str, column_name: str) -> Dict:
        """获取列的统计信息"""
        stats = {
            'total_count': 0,
            'null_count': 0,
            'distinct_count': 0,
            'null_ratio': 0.0
        }

        # 总行数
        query = f"SELECT COUNT(*) FROM `{table_name}`"
        self.cursor.execute(query)
        stats['total_count'] = self.cursor.fetchone()[0]

        # 空值数
        query = f"SELECT COUNT(*) FROM `{table_name}` WHERE `{column_name}` IS NULL"
        self.cursor.execute(query)
        stats['null_count'] = self.cursor.fetchone()[0]

        # 唯一值数量
        query = f"SELECT COUNT(DISTINCT `{column_name}`) FROM `{table_name}`"
        try:
            self.cursor.execute(query)
            stats['distinct_count'] = self.cursor.fetchone()[0]
        except Error:
            stats['distinct_count'] = 0

        # 计算空值率
        if stats['total_count'] > 0:
            stats['null_ratio'] = stats['null_count'] / stats['total_count']

        return stats

    # ==================== 第一层级：元数据过滤 ====================

    '''def is_primary_key_column(self, column_name: str) -> bool:
        """判断是否为主键列（基于命名）"""
        patterns = [r'^id$', r'^ID$', r'.*_id$', r'.*_ID$']
        return any(re.search(pattern, column_name) for pattern in patterns)'''

    def calculate_name_similarity(self, name1: str, name2: str) -> float:
        """计算字段名相似度（使用SequenceMatcher）"""
        return SequenceMatcher(None, name1.lower(), name2.lower()).ratio()

    def extract_table_name_from_fk(self, fk_column_name: str) -> str:
        """
        从外键列名中提取可能的表名
        例如: user_id -> user, customer_id -> customer, role_id -> role
        """
        # 移除 _id, _key 等后缀
        table_name = re.sub(r'[_-]?(id|key|Id|Key|ID|KEY)$', '', fk_column_name)
        # 转换为小写
        # table_name = table_name.lower()
        return table_name

    def should_skip_pair(self, fk_table: str, fk_column: str,
                        pk_table: str, pk_column: str) -> bool:
        """
        元数据过滤：判断是否应该跳过这对列的组合
        返回True表示应该跳过（不可能是外键关系）

        只对字段名为单纯的 'ID' 的情况进行严格检查
        """
        # 只对字段名为 ID 或 id 的情况进行特殊处理
        if fk_column.upper() == 'ID':
            # ID 字段通常只与同表或密切相关表匹配
            # 检查表名是否相关
            similarity = self.calculate_name_similarity(fk_table.lower(), pk_table.lower())
            if similarity < 0.3:
                return True

        return False

    def is_boolean_column(self, table: str, column: str) -> bool:
        """
        检测字段是否是布尔类型
        暂时只判定数据只包含 0、1、NULL 的字段是布尔字段
        """
        try:
            # 获取样本值检查
            values = self.get_column_sample_values(table, column, limit=100)
            # 过滤掉 NULL 值
            non_null_values = {v for v in values if v is not None}
            # 如果只有 0 和 1，判定为布尔字段
            return non_null_values == {0, 1} or non_null_values == {0} or non_null_values == {1}
        except:
            return False

    # ==================== 第二层级：数值分布分析 ====================

    def calculate_inclusion_dependency(self, fk_table: str, fk_column: str,
                                      pk_table: str, pk_column: str,
                                      sample_size: int = 1000) -> Dict:
        """
        计算包含依赖（Inclusion Dependency）
        检查 fk_table.fk_column 的值是否都包含在 pk_table.pk_column 中
        """
        # 获取样本值
        fk_values = self.get_column_sample_values(fk_table, fk_column, sample_size)
        pk_values = self.get_column_sample_values(pk_table, pk_column, sample_size)

        if not fk_values:
            return {'coverage': 0.0, 'fk_distinct': 0, 'pk_distinct': 0}

        # 计算覆盖率
        coverage = len(fk_values & pk_values) / len(fk_values) if fk_values else 0

        return {
            'coverage': coverage,
            'fk_distinct': len(fk_values),
            'pk_distinct': len(pk_values),
            'fk_values': fk_values,
            'pk_values': pk_values
        }

    def calculate_cardinality_ratio(self, fk_table: str, fk_column: str,
                                   pk_table: str, pk_column: str) -> float:
        """
        计算基数比率
        外键的基数应该 <= 主键的基数
        """
        fk_stats = self.get_column_stats(fk_table, fk_column)
        pk_stats = self.get_column_stats(pk_table, pk_column)

        if pk_stats['distinct_count'] == 0:
            return 0.0

        return fk_stats['distinct_count'] / pk_stats['distinct_count']

    # ==================== 第三层级：图论优化 ====================

    def resolve_conflicts(self, candidate_relationships: List[Dict]) -> List[Dict]:
        """
        解决冲突依赖
        如果一个外键列匹配多个主表，根据业务上下文选择最合适的
        """
        # 按外键表和列分组
        fk_groups = defaultdict(list)
        for rel in candidate_relationships:
            key = (rel['fk_table'], rel['fk_column'])
            fk_groups[key].append(rel)

        resolved_relationships = []

        for fk_key, candidates in fk_groups.items():
            if len(candidates) == 1:
                # 没有冲突，直接添加
                resolved_relationships.append(candidates[0])
            else:
                # 有冲突，选择最佳匹配
                # 优先级：1. 覆盖率 2. 表名相似度 3. 主键是否是主键
                best_candidate = max(candidates, key=lambda x: (
                    x['coverage'],
                    x['name_similarity'],
                    x['pk_is_primary']
                ))
                resolved_relationships.append(best_candidate)

        return resolved_relationships

    # ==================== 过滤辅助方法 ====================

    def _filter_tables(self, tables: List[str], include_tables: list = None,
                      exclude_tables: list = None) -> List[str]:
        """
        过滤表列表

        Args:
            tables: 所有表列表
            include_tables: 包含的表列表（None表示不限制）
            exclude_tables: 排除的表列表（None表示不限制）

        Returns:
            过滤后的表列表
        """
        filtered = tables

        # 先应用包含过滤
        if include_tables is not None and include_tables:
            include_tables = [t for t in include_tables if t not in ['None', 'none', '']]
            filtered = [t for t in filtered if t in include_tables]

        # 再应用排除过滤
        if exclude_tables is not None and exclude_tables:
            exclude_tables = [t for t in exclude_tables if t not in ['None', 'none', '']]
            filtered = [t for t in filtered if t not in exclude_tables]

        return filtered

    def _filter_columns(self, columns: List[Dict], table_name: str,
                       include_columns: list = None, exclude_columns: list = None) -> List[Dict]:
        """
        过滤列列表

        Args:
            columns: 所有列信息列表
            table_name: 当前表名
            include_columns: 包含的字段列表（格式：["table.column", ...]，None表示不限制）
            exclude_columns: 排除的字段列表（格式：["table.column", ...]，None表示不限制）

        Returns:
            过滤后的列列表
        """
        filtered = columns

        if include_columns is not None and include_columns:
            include_columns = [c for c in include_columns if c not in ['None', 'none', '']]
            # 提取属于当前表的包含列
            table_include_columns = [c.split('.')[-1] for c in include_columns
                                    if c.startswith(f'{table_name}.') or c == table_name]
            if table_include_columns:
                filtered = [c for c in filtered if c['name'] in table_include_columns]

        if exclude_columns is not None and exclude_columns:
            exclude_columns = [c for c in exclude_columns if c not in ['None', 'none', '']]
            # 提取属于当前表的排除列
            table_exclude_columns = [c.split('.')[-1] for c in exclude_columns
                                    if c.startswith(f'{table_name}.') or c == table_name]
            if table_exclude_columns:
                filtered = [c for c in filtered if c['name'] not in table_exclude_columns]

        return filtered

    # ==================== 核心算法：发现所有隐式外键 ====================

    def discover_implicit_foreign_keys(self, coverage_threshold: float = 0.85,
                                      max_null_ratio: float = 0.5,
                                      include_tables: list = None,
                                      exclude_tables: list = None,
                                      include_columns: list = None,
                                      exclude_columns: list = None) -> List[Dict]:
        """
        发现所有隐式外键

        Args:
            coverage_threshold: 覆盖率阈值（默认0.85，即85%）
            max_null_ratio: 最大空值率（默认0.5，即50%）
            include_tables: 包含的表列表（None表示不限制）
            exclude_tables: 排除的表列表（None表示不限制）
            include_columns: 包含的字段列表（格式：["table.column", ...]，None表示不限制）
            exclude_columns: 排除的字段列表（格式：["table.column", ...]，None表示不限制）

        Returns:
            发现的隐式外键关系列表
        """
        if not self.connect():
            return []

        logger.info("开始发现隐式外键...")

        # 1. 获取所有表并应用过滤
        all_tables = self.get_all_tables()

        # 应用表过滤
        tables = self._filter_tables(all_tables, include_tables, exclude_tables)
        logger.info(f"过滤后剩余 {len(tables)} 个表（原始 {len(all_tables)} 个）")

        table_columns = {}
        table_primary_keys = {}

        for table in tables:
            all_columns = self.get_table_columns(table)
            # 应用列过滤
            filtered_columns = self._filter_columns(all_columns, table, include_columns, exclude_columns)
            table_columns[table] = filtered_columns
            table_primary_keys[table] = self.get_primary_keys(table)

        # 2. 构建候选外键和候选主键列表
        candidate_fks = []  # 候选外键列
        candidate_pks = []  # 候选主键列

        for table in tables:
            for column in table_columns[table]:
                col_info = {
                    'table': table,
                    'column': column['name'],
                    'type': column['column_type'],
                    'data_type': column['data_type'],
                    'is_primary': column['column_key'] == 'PRI'
                }

                # 添加到候选外键（所有字段都可能是外键）
                candidate_fks.append(col_info)

                # 添加到候选主键（所有字段都可能是被引用的主键/候选键）
                # 这样可以发现如 customer_name -> customer_name 这样的关联关系
                candidate_pks.append(col_info)

        logger.info(f"候选外键列数: {len(candidate_fks)}")
        logger.info(f"候选主键列数: {len(candidate_pks)}")

        # 3. 对每对候选列进行验证
        candidate_relationships = []
        checked_pairs = set()  # 记录已检查的字段对，避免重复

        for fk in candidate_fks:
            for pk in candidate_pks:
                # 跳过同一张表
                if fk['table'] == pk['table']:
                    continue

                # 去重：跳过已检查的字段对（使用排序确保一致性）
                pair_key = tuple(sorted([
                    (fk['table'], fk['column']),
                    (pk['table'], pk['column'])
                ]))
                if pair_key in checked_pairs:
                    continue
                checked_pairs.add(pair_key)

                # 元数据过滤：数据类型必须兼容
                if fk['data_type'] != pk['data_type']:
                    continue

                # 元数据过滤：命名规则检查（仅对 ID 字段进行严格检查）
                if self.should_skip_pair(fk['table'], fk['column'],
                                        pk['table'], pk['column']):
                    continue

                # 跳过布尔字段对（只包含 0、1、NULL 的字段）
                # 布尔字段的覆盖率会虚假地接近 100%
                if self.is_boolean_column(fk['table'], fk['column']) or \
                   self.is_boolean_column(pk['table'], pk['column']):
                    continue

                # 计算命名相似度（仅用于优先级排序，不作为过滤条件）
                fk_extracted = self.extract_table_name_from_fk(fk['column'])
                name_similarity = self.calculate_name_similarity(fk_extracted, pk['table'])

                # 数值分析：计算包含依赖
                try:
                    print(f"DISCOVERING {fk['table']}.{fk['column']} AND {pk['table']}.{pk['column']}")
                    inclusion = self.calculate_inclusion_dependency(
                        fk['table'], fk['column'],
                        pk['table'], pk['column']
                    )

                    # 检查覆盖率阈值
                    if inclusion['coverage'] < coverage_threshold:
                        continue

                    # 获取外键的空值率
                    fk_stats = self.get_column_stats(fk['table'], fk['column'])

                    # 检查空值率
                    if fk_stats['null_ratio'] > max_null_ratio:
                        continue

                    # 计算基数比率
                    card_ratio = self.calculate_cardinality_ratio(
                        fk['table'], fk['column'],
                        pk['table'], pk['column']
                    )

                    # 记录候选关系
                    candidate_relationships.append({
                        'fk_table': fk['table'],
                        'fk_column': fk['column'],
                        'pk_table': pk['table'],
                        'pk_column': pk['column'],
                        'coverage': inclusion['coverage'],
                        'fk_distinct': inclusion['fk_distinct'],
                        'pk_distinct': inclusion['pk_distinct'],
                        'null_ratio': fk_stats['null_ratio'],
                        'cardinality_ratio': card_ratio,
                        'name_similarity': name_similarity,
                        'pk_is_primary': pk['is_primary'],
                        'fk_type': fk['type'],
                        'pk_type': pk['type']
                    })
                    print(f"发现候选关系: {fk['table']}.{fk['column']} -> {pk['table']}.{pk['column']} "
                              f"(覆盖率: {inclusion['coverage']:.2%}, "
                              f"相似度: {name_similarity:.2f})")
                    logger.info(f"发现候选关系: {fk['table']}.{fk['column']} -> {pk['table']}.{pk['column']} "
                              f"(覆盖率: {inclusion['coverage']:.2%}, "
                              f"相似度: {name_similarity:.2f})")

                except Exception as e:
                    logger.warning(f"分析 {fk['table']}.{fk['column']} -> {pk['table']}.{pk['column']} 时出错: {e}")
                    continue

        logger.info(f"发现 {len(candidate_relationships)} 个候选关系（冲突解决前）")

        # 4. 解决冲突依赖
        final_relationships = self.resolve_conflicts(candidate_relationships)

        logger.info(f"最终发现 {len(final_relationships)} 个隐式外键关系")

        self.close()
        return final_relationships


def main():
    """主函数：发现隐式外键并输出结果"""
    # 数据库配置
    '''discoverer = ImplicitFKDiscoverer(
        host='172.31.24.111',
        port=3307,
        user='root',
        password='liucd123',
        database='netcare'
    )'''

    discoverer = ImplicitFKDiscoverer(
        host='172.31.26.206',
        port=3306,
        user='ai_test',
        password='Netcare@13579',
        database='netcaredb_ai'
    )

    # 发现隐式外键
    # coverage_threshold=0.85: 覆盖率至少85%
    # max_null_ratio=0.5: 空值率不超过50%
    relationships = discoverer.discover_implicit_foreign_keys(
        coverage_threshold=0.85,
        max_null_ratio=0.5
    )

    # 输出结果
    output_file = '/data/liyiru/mysql-graph/implicit_foreign_keys.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(relationships, f, indent=2, ensure_ascii=False)

    logger.info(f"结果已保存到: {output_file}")

    # 打印摘要
    print("\n" + "="*80)
    print("隐式外键发现结果摘要")
    print("="*80)
    print(f"总共发现 {len(relationships)} 个隐式外键关系\n")

    for i, rel in enumerate(relationships, 1):
        print(f"{i}. {rel['fk_table']}.{rel['fk_column']} -> {rel['pk_table']}.{rel['pk_column']}")
        print(f"   覆盖率: {rel['coverage']:.2%} | "
              f"空值率: {rel['null_ratio']:.2%} | "
              f"基数比: {rel['cardinality_ratio']:.2f} | "
              f"相似度: {rel['name_similarity']:.2f}")
        print()

    return relationships


if __name__ == "__main__":
    main()
