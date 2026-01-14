
from __future__ import annotations
import sys
import os
import pandas as pd
import numpy as np
from typing import Any, Dict, List

# 添加项目根目录到sys.path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.insert(0, project_root)

from DataAgent.workflow.nodes.node_factory import register_node
from DataAgent.workflow.nl2flow.workflow_state import WorkflowState


# ========================================================================
# SQL算子 (数据库结构化查询算子)
# ========================================================================


@register_node(
    name="sql",
    description="查询数据库中的结构化数据",
)
async def sql(state: WorkflowState, ) -> WorkflowState:
    """
    sql算子 - 对表数据进行字段过滤
    返回： 对sql执行的结果保存到视图
    """
    # 使用 state.current_node 获取当前节点唯一名称 (如 sql_0, sql_1)
    current_node = state.current_node
    idx = state.sub_node_name.index(current_node)
    instruction = state.sub_node_instruction[idx]
    print(f'SQL Node [{current_node}]:', instruction)

    # Text2sql
    # sql_sentence = text2sql(instruction)
    sql_sentence = f"SELECT * FROM table_name where x (from {current_node})"

    state.sub_node_result.append(sql_sentence)

    return state


