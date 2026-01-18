
a = "[{'id': 4, 'database_info_name': '星辰二期', 'status': 0, 'database_type': 'MySQL', 'host': '172.31.24.111', 'port': 3307, 'user_name': 'root', 'database_name': '12345', 'driver': '1', 'password': 'WCLhRfXXAXuZEPRwEuK0IA==', 'pool_size': 0, 'max_overflow': 0, 'pool_tmout': 0, 'pool_recycle': 0, 'pool_pre_ping': 0, 'description': '星辰二期数据库', 'create_time': None, 'update_time': datetime.datetime(2025, 11, 12, 6, 0, 3), 'selected_tables': 'shanghai', 'is_semantic_analysis': 1, 'primary_key_name': '工单编号', 'unstructrued_column': '内容描述', 'large_step': '150', 'small_step': '10', 'window_size': '50', 'min_samples': '5', 'max_samples': '1000', 'target_table': 'shanghai', 'milvus_status': None, 'is_deleted': 1, 'user_id': 1}]"
import ast
import datetime

# 使用 ast.literal_eval() 解析 Python 字符串表示
result = ast.literal_eval(a)
print("成功解析！")
print(f"结果类型: {type(result)}")
print(f"第一条数据: {result[0]}")