
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
