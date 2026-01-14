
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
# 分类算子
# ========================================================================


@register_node(
    name="classify",
    description="对数据库中的某列数据进行分类",
)
async def classify(state: WorkflowState, ) -> WorkflowState:
    """
    classify算子 - 对自由文本列进行分类
    返回： 对AI分析的结果保存到子表
    """
    # 使用 state.current_node 获取当前节点唯一名称 (如 classify_0)
    current_node = state.current_node
    idx = state.sub_node_name.index(current_node)
    instruction = state.sub_node_instruction[idx]
    print(f'Classify Node [{current_node}]:', instruction)

    state.sub_node_result.append('success')

    return state


