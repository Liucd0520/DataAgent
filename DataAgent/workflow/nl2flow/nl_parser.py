import sys
import os
import pandas as pd

# 添加项目根目录到sys.path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.insert(0, project_root)

import json
from typing import List
from DataAgent.workflow.chain import planner_chain
from config.config import schema, sementic_field

# 查询解析函数函数
def parse_workflow(query: str) -> List[dict]:
    """
    解析自然语言查询为工作流节点列表

    Args:
        query: 自然语言查询

    Returns:
        节点名称列表
    Example:
        nodes = parse_workflow("近10个月垃圾分类有关投诉事件主要分布在哪些区")
        [
            {
                "op": "sql",
                "instruction": "筛选近10个月内的所有工单，其中工单类型为'投诉举报类'，并保留工单编号、诉求区域 、内容描述字段",
                "input_fields": [],
                "output_fields": []
            },
            {
                "op": "semantic_filter",
                "instruction": "从内容描述中筛选出与'垃圾分类'语义相关的投诉记录",
                "input_fields": ["内容描述"],
                "output_fields": ["is_garbage_related"]
            },
            {
                "op": "sql",
                "instruction": "对通过语义过滤的垃圾分类相关投诉记录，按诉求区域分组统计数量，并按数量降序排列 ，返回区域名称和投诉数量",
                "input_fields": [],
                "output_fields": []
            }
        ]

    """
    
    # 调用大模型
    response = planner_chain.invoke({"query": query, "schema": schema, "sementic_field": sementic_field})
    
    # 解析响应
    try:
        nodes_pipeline = json.loads(response.content)
        
        print(f"Parsed query: '{query}' -> {len(nodes_pipeline)} nodes: \n {nodes_pipeline}")
        return nodes_pipeline

    except json.JSONDecodeError as e:
        print(f"[Error] 无法解析出正确的算子流: {e}")
        return []

if __name__ == '__main__':
    result = parse_workflow('最近30个月的群租现象的投诉有多少起')
    print(result)
